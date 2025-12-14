#!/usr/bin/env python3

# using python 3.9 
    
from bluepy.btle import Peripheral, DefaultDelegate, BTLEException
import struct
import argparse
import json
import time
import binascii
import atexit
 
     # Command line arguments
z = 10
meter = "casa-dchouse-48v"    
ble_device = 'A4:C1:37:55:38:07'
cells1 = []


from prometheus_client import Histogram, CollectorRegistry, start_http_server, Gauge, Info, generate_latest
registry = CollectorRegistry()
port = 9658
metrics = {
        'cell': Gauge('battery_cell', 
                     'Battery cell metrics. Metric types: volts (cell voltage in mV, cell=01-16), balance (balancing status 0/1, cell=01-16), '
                     'percent (state of charge, cell=battery), fet (FET status: 0=both off, 1=charge on, 2=discharge on, 3=both on, cell=battery), '
                     'temp1-4 (temperature in Celsius, cell=temp). Protection statuses (cell=protect, value 0=off/1=on): '
                     'ovp=Overvoltage Protection, uvp=Undervoltage Protection, bov=Battery Pack Overvoltage, buv=Battery Pack Undervoltage, '
                     'cot=Charge Over Temperature, cut=Charge Under Temperature, dot=Discharge Over Temperature, dut=Discharge Under Temperature, '
                     'coc=Charge Over Current, duc=Discharge Under Current, sc=Short Circuit, ic=IC Failure, cnf=Configuration Problem', 
                     labelnames=['meter', 'cell', 'metric'], registry=registry),
        'volts': Gauge('battery_volt', 'Total pack voltage in volts',  labelnames=['meter'], registry=registry),
        'amps': Gauge('battery_amps', 'Pack current in amperes (positive=charging, negative=discharging)',  labelnames=['meter'], registry=registry),
        'watts': Gauge('battery_watts', 'Pack power in watts (volts * amps)',  labelnames=['meter'], registry=registry),
        'remain': Gauge('battery_remain', 'Remaining capacity in amp-hours',  labelnames=['meter'], registry=registry),
        'capacity': Gauge('battery_capacity', 'Total pack capacity in amp-hours',  labelnames=['meter'], registry=registry),
        'cycles': Gauge('battery_cycles', 'Number of charge/discharge cycles',  labelnames=['meter'], registry=registry),
        'cellmin': Gauge('battery_cellmin', 'Cell number with minimum voltage',  labelnames=['meter'], registry=registry),
        'cellmax': Gauge('battery_cellmax', 'Cell number with maximum voltage',  labelnames=['meter'], registry=registry),
        'delta': Gauge('battery_delta', 'Voltage difference between max and min cells in millivolts',  labelnames=['meter'], registry=registry),
        'hwversion': Info('battery_hwversion', 'BMS hardware version information', labelnames=['meter'], registry=registry),
}

def disconnect():
    print("broker disconnected")

def cellinfo1(data):            # process pack info
    infodata = data
    i = 4                       # Unpack into variables, skipping header bytes 0-3
    volts, amps, remain, capacity, cycles, mdate, balance1, balance2 = struct.unpack_from('>HhHHHHHH', infodata, i)
    volts=volts/100

    amps = amps/100
    capacity = capacity/100
    remain = remain/100
    watts = volts*amps                              # adding watts field for dbase
    message1 = {
        "meter": "bms",
        "volts": volts,
        "amps": amps,
        "watts": watts, 
        "remain": remain, 
        "capacity": capacity, 
        "cycles": cycles 
    }

    metrics['volts'].labels(meter).set(volts)
    metrics['amps'].labels(meter).set(amps)
    metrics['watts'].labels(meter).set(watts)
    metrics['remain'].labels(meter).set(remain)
    metrics['capacity'].labels(meter).set(capacity)
    metrics['cycles'].labels(meter).set(cycles)

    
    bal1 = (format(balance1, "b").zfill(16))        
    message2 = {
        "meter": "bms",                            # using balance1 bits for 16 cells
        "c16" : int(bal1[0:1]), 
        "c15" : int(bal1[1:2]),                 # balance2 is for next 17-32 cells - not using
        "c14" : int(bal1[2:3]),                             
        "c13" : int(bal1[3:4]), 
        "c12" : int(bal1[4:5]),                 # bit shows (0,1) charging on-off            
        "c11" : int(bal1[5:6]), 
        "c10" : int(bal1[6:7]), 
        "c09" : int(bal1[7:8]), 
        "c08" : int(bal1[8:9]), 
        "c07" : int(bal1[9:10]), 
        "c06" : int(bal1[10:11]),         
        "c05" : int(bal1[11:12]), 
        "c04" : int(bal1[12:13]) , 
        "c03" : int(bal1[13:14]), 
        "c02" : int(bal1[14:15]), 
        "c01" : int(bal1[15:16])
    }

    for cell_id in range(16):
        a = 15-cell_id
        b = 16-cell_id
        metrics['cell'].labels(meter, str(f'{cell_id+1:02d}'), 'balance').set(int(bal1[a:b]))


def cellinfo2(data):
    infodata = data
    i = 0                          # unpack into variables, ignore end of message byte '77'
    protect,vers,percent,fet,cells,sensors,temp1,temp2,temp3,temp4,b77 = struct.unpack_from('>HBBBBBHHHHB', infodata, i)
    temp1 = (temp1-2731)/10
    temp2 = (temp2-2731)/10            # fet 0011 = 3 both on ; 0010 = 2 disch on ; 0001 = 1 chrg on ; 0000 = 0 both off
    temp3 = (temp3-2731)/10
    temp4 = (temp4-2731)/10
    prt = (format(protect, "b").zfill(16))        # protect trigger (0,1)(off,on)
    message1 = {
        "meter": "bms",
        "ovp" : int(prt[0:1]),             # overvoltage
        "uvp" : int(prt[1:2]),             # undervoltage
        "bov" : int(prt[2:3]),         # pack overvoltage
        "buv" : int(prt[3:4]),            # pack undervoltage
        "cot" : int(prt[4:5]),        # current over temp
        "cut" : int(prt[5:6]),            # current under temp
        "dot" : int(prt[6:7]),            # discharge over temp
        "dut" : int(prt[7:8]),            # discharge under temp
        "coc" : int(prt[8:9]),        # charge over current
        "duc" : int(prt[9:10]),        # discharge under current
        "sc" : int(prt[10:11]),        # short circuit
        "ic" : int(prt[11:12]),        # ic failure
        "cnf" : int(prt[12:13])        # config problem
    }

    message2 = {
        "meter": "bms",
        "protect": protect,
        "percent": percent,
        "fet": fet,
        "cells": cells,
        "temp1": temp1,
        "temp2": temp2,
        "temp3": temp3,
        "temp4": temp4,
    }

    # Add metrics for cellinfo2 data
    metrics['cell'].labels(meter, 'battery', 'percent').set(percent)
    metrics['cell'].labels(meter, 'battery', 'fet').set(fet)
    metrics['cell'].labels(meter, 'temp', 'temp1').set(temp1)
    metrics['cell'].labels(meter, 'temp', 'temp2').set(temp2)
    metrics['cell'].labels(meter, 'temp', 'temp3').set(temp3)
    metrics['cell'].labels(meter, 'temp', 'temp4').set(temp4)

    # Add protection status metrics
    for prot_name, prot_bit in [
        ('ovp', 0), ('uvp', 1), ('bov', 2), ('buv', 3),
        ('cot', 4), ('cut', 5), ('dot', 6), ('dut', 7),
        ('coc', 8), ('duc', 9), ('sc', 10), ('ic', 11), ('cnf', 12)
    ]:
        metrics['cell'].labels(meter, 'protect', prot_name).set(int(prt[prot_bit:prot_bit+1]))


def cellinfo3(data):
    """
    Process extended pack information from bytes 20-40 of extended dd03 message.
    
    This function handles the extended sensor data that appears in longer BLE messages.
    It extracts protection status and temperature readings from the middle portion
    of a multi-part message.
    
    Args:
        data (bytes): Extended data segment (typically bytes 20-40 from full message)
                     Format: [00 00][protect H][byte4 B][cells B][sensors B][temp1 H][temp2 H][temp3 H][...]
    
    Data format:
        - Bytes 0-1: Reserved/padding (00 00)
        - Bytes 2-3: Protection status (16-bit, same format as cellinfo2)
        - Byte 4: Unknown/configuration byte
        - Byte 5: Number of cells
        - Byte 6: Number of temperature sensors
        - Bytes 7-12: Temperature readings (2 bytes each, deci-Kelvin format)
    
    Example hex data: 000024640310030b2e0b240b240000001388137a
    Decoded: skip(00 00) | protect(24 64) | config(03) | cells(10) | sensors(03) | temp1(0b 2e) | temp2(0b 24) | temp3(0b 24) | ...
    """
    if len(data) < 14:
        return

    try:
        # Extract protection status (bytes 2-3)
        protect = struct.unpack_from('>H', data, 2)[0]
        
        # Extract cell and sensor counts
        cells = struct.unpack_from('>B', data, 5)[0]
        sensors = struct.unpack_from('>B', data, 6)[0]

        # Decode protection bits (same format as cellinfo2)
        prt = (format(protect, "b").zfill(16))

        # Process temperatures if we have at least 3 sensors
        if sensors >= 3:
            temp1, temp2, temp3 = struct.unpack_from('>HHH', data, 7)

            # Sanity check: temperatures should be in deci-Kelvin format
            # Valid range: 2500-3500 deci-Kelvin (approximately -23°C to +77°C)
            if 2500 < temp1 < 3500 and 2500 < temp2 < 3500 and 2500 < temp3 < 3500:
                # Convert from deci-Kelvin to Celsius
                temp1_c = (temp1 - 2731) / 10
                temp2_c = (temp2 - 2731) / 10
                temp3_c = (temp3 - 2731) / 10

                # Update Prometheus metrics
                metrics['cell'].labels(meter, 'temp', 'temp1').set(temp1_c)
                metrics['cell'].labels(meter, 'temp', 'temp2').set(temp2_c)
                metrics['cell'].labels(meter, 'temp', 'temp3').set(temp3_c)

                # Update protection status metrics (same format as cellinfo2)
                for prot_name, prot_bit in [
                    ('ovp', 0), ('uvp', 1), ('bov', 2), ('buv', 3),
                    ('cot', 4), ('cut', 5), ('dot', 6), ('dut', 7),
                    ('coc', 8), ('duc', 9), ('sc', 10), ('ic', 11), ('cnf', 12)
                ]:
                    metrics['cell'].labels(meter, 'protect', prot_name).set(int(prt[prot_bit:prot_bit+1]))
    except Exception:
        pass

def hwversion(data):
    """
    Process hardware version response (command 0x05).
    
    Decodes the hardware version string returned by the BMS.
    The version is an ASCII string that can be up to 31 characters long.
    
    Args:
        data (bytes): Raw BLE notification data from command 0x05
                     Format: [dd][05][status][length][version_string...][checksum][77]
                     - dd: Start byte
                     - 05: Response type (some BMS may use 04)
                     - status: 0x00 = success
                     - length: Length of version string
                     - version_string: ASCII characters
                     - checksum: Data validation (2 bytes)
                     - 77: End marker
    
    The hardware version is typically read once at startup and doesn't change,
    so it's exported as a Prometheus Info metric (key-value labels).
    """
    if len(data) < 5:
        return
    
    try:
        # Response format: dd 05 [status] [length] [version_string...] [checksum] 77
        # Or possibly: dd 04 [status] [length] [version_string...] [checksum] 77
        # Skip first 2 bytes (dd 05 or dd 04), then status byte, then length byte
        status = data[2]
        length = data[3]
        
        if status != 0x00:
            return
        
        # Extract version string (skip header: dd 05/04 status length = 4 bytes)
        # Version string ends before checksum (2 bytes) and 77 marker (1 byte)
        # Need at least: 4 header bytes + length bytes + 2 checksum bytes + 1 end marker
        min_required = 4 + length + 3
        if length > 0 and len(data) >= min_required:
            version_bytes = data[4:4+length]
            version_string = version_bytes.decode('ascii', errors='replace').strip()
            
            # Update Prometheus Info metric
            metrics['hwversion'].labels(meter).info({'version': version_string})
        else:
            # Try to extract anyway if we have at least some data
            if len(data) > 4:
                try:
                    # Try extracting up to available length (leave room for 2-byte checksum and 77)
                    extract_len = min(length, len(data) - 4 - 3)
                    if extract_len > 0:
                        version_bytes = data[4:4+extract_len]
                        version_string = version_bytes.decode('ascii', errors='replace').strip()
                        metrics['hwversion'].labels(meter).info({'version': version_string})
                except Exception:
                    pass
            
    except Exception:
        pass

def cellvolts1(data):
    """
    Process cell voltages for cells 1-8 (command 0x04, first part).
    
    Decodes the first 8 cell voltages from the BMS. These values are stored
    globally for later calculation of min/max/delta when combined with cellvolts2.
    
    Args:
        data (bytes): Raw BLE notification data starting with 'dd04'
                     Format: [header 4 bytes][cell1 H][cell2 H][...][cell8 H]
                     Each cell voltage is in millivolts (unsigned 16-bit big-endian)
    
    Global variables:
        cells1: Updated with first 8 cell voltages for delta calculations
    """
    global cells1
    celldata = data
    i = 4  # Skip header bytes 0-3 (dd, a5, 04, 00)
    
    # Unpack 8 cell voltages (each is unsigned 16-bit, big-endian)
    # Values are in millivolts
    cell1, cell2, cell3, cell4, cell5, cell6, cell7, cell8 = struct.unpack_from('>HHHHHHHH', celldata, i)
    
    # Store in global list for later min/max/delta calculations with cells 9-16
    cells1 = [cell1, cell2, cell3, cell4, cell5, cell6, cell7, cell8]
    
    # Create JSON message (for potential future use)
    message = {
        "meter": "bms", 
        "cell1": cell1, 
        "cell2": cell2,
        "cell3": cell3, 
        "cell4": cell4,
        "cell5": cell5, 
        "cell6": cell6, 
        "cell7": cell7, 
        "cell8": cell8 
    }

    # Update Prometheus metrics for each cell voltage
    cell_volts = struct.unpack_from('>HHHHHHHH', celldata, i)
    for cell_id in range(8):
        metrics['cell'].labels(meter, str(f'{cell_id+1:02d}'), 'volts').set(cell_volts[cell_id])


def cellvolts2(data):
    """
    Process cell voltages for cells 9-16 (command 0x04, second part).
    
    Decodes the last 8 cell voltages and calculates pack statistics including
    min/max cell voltages and voltage delta (difference between highest and lowest cell).
    
    Args:
        data (bytes): Raw BLE notification data ending with '77'
                     Format: [cell9 H][cell10 H][...][cell16 H][77 B]
                     Each cell voltage is in millivolts (unsigned 16-bit big-endian)
    
    Global variables:
        cells1: Used from cellvolts1() to combine all 16 cells for statistics
    
    Calculated metrics:
        - delta: Voltage difference between max and min cells (in millivolts)
        - cellmin: Cell number with minimum voltage (1-16)
        - cellmax: Cell number with maximum voltage (1-16)
    """
    celldata = data
    i = 0  # Start from beginning, no header to skip
    
    # Unpack 8 cell voltages plus end-of-message marker
    # B = unsigned byte (0x77 end marker)
    cell9, cell10, cell11, cell12, cell13, cell14, cell15, cell16, b77 = struct.unpack_from('>HHHHHHHHB', celldata, i)
    
    # Create JSON message (for potential future use)
    message = {
        "meter": "bms", 
        "cell9": cell9, 
        "cell10": cell10, 
        "cell11": cell11, 
        "cell12": cell12,
        "cell13": cell13, 
        "cell14": cell14, 
        "cell15": cell15, 
        "cell16": cell16 
    }
    
    # Update Prometheus metrics for cells 9-16
    cell_volts = struct.unpack_from('>HHHHHHHHB', celldata, i)
    for cell_id in range(8):
        metrics['cell'].labels(meter, str(f'{cell_id+9:02d}'), 'volts').set(cell_volts[cell_id])

    # Combine all 16 cells for statistical calculations
    cells2 = [cell9, cell10, cell11, cell12, cell13, cell14, cell15, cell16]
    allcells = cells1 + cells2  # Combine with cells from cellvolts1()
    
    # Calculate pack statistics
    cellsmin = min(allcells)  # Minimum cell voltage in millivolts
    cellsmax = max(allcells)  # Maximum cell voltage in millivolts
    delta = cellsmax - cellsmin  # Voltage spread in millivolts
    
    # Identify which cells have min/max voltages (1-indexed)
    mincell = (allcells.index(min(allcells)) + 1)
    maxcell = (allcells.index(max(allcells)) + 1)

    # Update Prometheus metrics for pack statistics
    metrics['delta'].labels(meter).set(delta)
    metrics['cellmin'].labels(meter).set(mincell)
    metrics['cellmax'].labels(meter).set(maxcell)

class MyDelegate(DefaultDelegate):
    """
    BLE notification delegate for handling incoming data from the BMS.
    
    This class extends DefaultDelegate to receive BLE notifications from the JBD BMS.
    It handles multi-part messages (BLE has a 20-byte MTU limit) by buffering
    incomplete messages until the complete message is received (indicated by '77' end marker).
    
    Message types:
        - dd03: Pack information (voltage, current, capacity, balancing status)
        - dd04: Cell voltages (16 cells, split across multiple notifications)
        - dd05: Hardware version response (command 0x05 response)
        - Extended messages: Longer messages that span multiple BLE packets
    
    Message format:
        - Messages start with 'dd' followed by command byte (a5, 03/04)
        - Messages end with '77' (0x77) end marker
        - BLE MTU limit of 20 bytes means longer messages are split
    """
    
    def __init__(self):
        """Initialize the delegate with an empty message buffer."""
        DefaultDelegate.__init__(self)
        self.message_buffer = b''  # Buffer for reassembling multi-part BLE messages
        self.hw_version_received = False  # Track if hardware version has been received

    def handleNotification(self, cHandle, data):
        """
        Handle incoming BLE notifications from the BMS.
        
        This method is called automatically by bluepy when a notification is received.
        It buffers multi-part messages and routes complete messages to the appropriate
        decoder function based on message type and length.
        
        Args:
            cHandle (int): Characteristic handle (not used, but required by interface)
            data (bytes): Raw notification data (up to 20 bytes per BLE packet)
        
        Message routing logic:
            - Messages starting with 'dd' are new messages (may be multi-part)
            - Messages ending with '77' are complete
            - Extended dd04 (78 hex chars = 39 bytes): Split and process as cellvolts1 + cellvolts2
            - Extended dd03 (90 hex chars = 45 bytes): Split and process as cellinfo1 + cellinfo3
            - Standard dd04: Process as cellvolts1
            - Standard dd03: Process as cellinfo1
            - Standalone '77' messages: Process as cellvolts2 or cellinfo2 based on length
        """
        # Convert binary data to hex string for pattern matching
        hex_data = binascii.hexlify(data)
        text_string = hex_data.decode('utf-8')

        # Handle multi-part BLE messages
        # BLE has a 20-byte MTU (Maximum Transmission Unit) limit per packet,
        # so messages longer than 20 bytes are automatically split by the BLE stack
        if text_string.startswith('dd'):
            # Start of a new message - initialize buffer with this packet
            self.message_buffer = data
            # Check if this is a complete message (ends with '77' end marker)
            if text_string.endswith('77'):
                # Complete single-packet message, process immediately
                pass  # Fall through to routing logic below
            else:
                # Incomplete message, wait for continuation packets
                return
        elif len(self.message_buffer) > 0:
            # We're already buffering a multi-part message - append this continuation packet
            self.message_buffer += data
            if not text_string.endswith('77'):
                # Still waiting for more packets (message not complete yet)
                return
            # Message complete (ends with '77'), process the full reassembled message
            data = self.message_buffer
            hex_data = binascii.hexlify(data)
            text_string = hex_data.decode('utf-8')
            self.message_buffer = b''  # Clear buffer for next message
        else:
            # Unexpected data with no active buffered message
            if text_string == '00':
                # Single null byte - connection acknowledgment from BMS, ignore
                return
            return

        # Route complete messages to appropriate decoding routines
        # Message identification is based on:
        #   1. Message prefix (dd03 = pack info, dd04 = cell voltages, dd05 = hardware version)
        #   2. Message length (hex string length = 2 * byte length)
        #   3. End marker presence ('77')
        
        # Check for hardware version response (command 0x05 response starts with dd05)
        if text_string.startswith('dd05') and len(data) >= 5:
            # Hardware version response format: dd 05 [status] [length] [version_string...] [checksum] 77
            hwversion(data)
            self.hw_version_received = True
        # Check for hardware version response in dd04 format (before other dd04 messages)
        elif text_string.startswith('dd04') and len(data) >= 5:
            # Hardware version responses: dd 04 [status] [length] [version...] [checksum] 77
            # They're typically short messages with a length byte at position 3
            # Try to identify by checking if it has the structure of a version response
            status = data[2]
            length = data[3] if len(data) > 3 else 0
            
            # Hardware version responses have:
            # - status 0x00 (success)
            # - length byte < 32 (reasonable version string length)
            # - Different structure than cell voltage messages
            # Cell voltage messages typically have data starting at byte 4, not a length byte
            is_likely_hwversion = (
                status == 0x00 and 
                0 < length < 32 and 
                len(data) >= 4 + length + 2 and  # Has room for version + checksum + 77
                len(text_string) != 40 and  # Not standard cell voltage part 1
                len(text_string) != 78      # Not extended cell voltage
            )
            
            if is_likely_hwversion:
                hwversion(data)
                self.hw_version_received = True
            elif len(text_string) == 78:
                # Extended cell voltage message
                cellvolts1(data[:20])
                cellvolts2(data[20:])
            elif len(text_string) == 40:
                # Standard cell voltage part 1
                cellvolts1(data)
            else:
                # Unknown dd04 message - try as hardware version if it looks right
                if status == 0x00 and 0 < length < 32:
                    hwversion(data)
                    self.hw_version_received = True
                else:
                    # Fall back to cell voltage processing
                    cellvolts1(data)
        elif text_string.find('dd04') != -1 and len(text_string) == 78:
            # Extended cell voltage message: 39 bytes total (78 hex chars)
            # This is a complete message containing all 16 cells in one notification
            # Format: [20 bytes cellvolts1 data][19 bytes cellvolts2 data]
            cellvolts1(data[:20])   # First 20 bytes: cells 1-8
            cellvolts2(data[20:])   # Remaining 19 bytes: cells 9-16 + end marker
        elif text_string.find('dd03') != -1 and len(text_string) == 90:
            # Extended pack info message: 45 bytes total (90 hex chars)
            # This is a complete message with extended sensor data
            # Format: [20 bytes cellinfo1 data][20 bytes cellinfo3 data][5 bytes footer/checksum]
            cellinfo1(data[:20])    # First 20 bytes: standard pack info (volts, amps, capacity, balance)
            cellinfo3(data[20:40])  # Next 20 bytes: extended sensor data (temps, protection)
            # The last 5 bytes (0000fae177) appear to be a checksum or footer
            # NOTE: cellinfo2 data (temps, protect, percent, fet) is NOT in extended messages!
        elif text_string.find('dd04') != -1:
            # Standard cell voltages part 1: First 8 cells only
            # This is a split message - cellvolts2 will come in a separate notification
            cellvolts1(data)
        elif text_string.find('dd03') != -1:
            # Standard pack info: Basic pack information only
            # This is a split message - cellinfo2 will come in a separate notification
            cellinfo1(data)
        elif text_string.find('77') != -1 and len(text_string) == 38:
            # Cell voltages part 2: Last 8 cells (19 bytes = 38 hex chars)
            # This is the continuation of a split dd04 message
            cellvolts2(data)
        elif text_string.find('77') != -1 and len(text_string) == 36:
            # Pack info part 2: Extended info (18 bytes = 36 hex chars)
            # This is the continuation of a split dd03 message
            cellinfo2(data)

def connect():
    """
    Establish BLE connection to the JBD BMS device.
    
    Attempts to connect to the BMS using its MAC address. If connection fails,
    waits 10 seconds before returning None (caller should retry).
    
    Returns:
        Peripheral: Connected BLE peripheral object, or None if connection failed
    
    Side effects:
        - Registers disconnect() function with atexit for cleanup
        - Sets up MyDelegate for handling BLE notifications
    """
    try:
        print('attempting to connect')
        # Connect to BLE device using public address type
        # addrType="public" means the device uses a public MAC address
        bms = Peripheral(ble_device, addrType="public")
    except BTLEException as ex:
        # Connection failed - return None so caller can retry
        print('cannot connect, sleep 10')
        time.sleep(10)
        return None
    else:
        print('connected ', ble_device)

    # Register cleanup function to run on program exit
    atexit.register(disconnect)
    
    # Set up notification delegate to handle incoming BLE data
    bms.setDelegate(MyDelegate())

    return bms

if __name__ == "__main__":
    """
    Main execution loop for BMS monitoring.
    
    Flow:
        1. Connect to BMS (retry until successful)
        2. Start Prometheus HTTP server for metrics export
        3. Enter main loop:
           a. Request cell voltages (command 0x04)
           b. Wait for notification response
           c. Request pack info (command 0x03)
           d. Wait for notification response
           e. Sleep for configured interval
        4. Handle disconnections with automatic reconnection
    
    BLE Commands:
        - 0x15: Characteristic handle for sending commands
        - dd a5 04 00 ff fc 77: Request cell voltages (0x04)
        - dd a5 03 00 ff fd 77: Request pack info (0x03)
        - dd a5 05 00 ff fb 77: Request hardware version (0x05, requested once at startup)
        - waitForNotifications(5): Wait up to 5 seconds for response
          (Using 5 seconds prevents missed notifications with shorter timeouts)
    """
    # Connect to BMS (retry until successful)
    bms = None
    while not bms:
        bms = connect()

    # Start Prometheus HTTP server on configured port
    # Metrics will be available at http://localhost:9658/metrics
    start_http_server(port, registry=registry)
    print(f'Prometheus metrics server started on port {port}')

    # Request hardware version once at startup
    # Command format: dd a5 [command] 00 [checksum] 77
    # Checksum pattern analysis:
    #   0x04: dd a5 04 00 = 0x01a6, checksum = ff fc
    #   0x03: dd a5 03 00 = 0x01a5, checksum = ff fd
    #   0x05: dd a5 05 00 = 0x01a7, checksum = ff fb (following pattern)
    # The pattern suggests: checksum = 0xff (0x100 - (sum & 0xff) - 1)
    # Request hardware version once at startup
    try:
        result = bms.writeCharacteristic(0x15, b'\xdd\xa5\x05\x00\xff\xfb\x77', False)
        # Wait multiple times to catch the response (it may come in separate notifications)
        for i in range(5):
            bms.waitForNotifications(2)  # Wait up to 2 seconds per cycle
        time.sleep(0.5)  # Final brief pause
    except Exception:
        pass

    # Main monitoring loop
    while True:
        try:
            # Request cell voltages (command 0x04)
            # Command format: dd a5 [command] 00 [checksum] 77
            # False = write without response (notification will come separately)
            result = bms.writeCharacteristic(0x15, b'\xdd\xa5\x04\x00\xff\xfc\x77', False)
            bms.waitForNotifications(5)  # Wait up to 5 seconds for response
            
            # Request pack info (command 0x03)
            # This gets voltage, current, capacity, balancing status, etc.
            result = bms.writeCharacteristic(0x15, b'\xdd\xa5\x03\x00\xff\xfd\x77', False)
            bms.waitForNotifications(5)  # Wait up to 5 seconds for response
            
            # Sleep before next cycle
            time.sleep(z)
            
        except BTLEException as ex:
            # Handle BLE disconnection - attempt to reconnect
            print(f'BLE disconnected: {ex}')
            print('Attempting to reconnect...')
            bms = None
            while not bms:
                bms = connect()
                if not bms:
                    time.sleep(5)  # Wait 5 seconds before retry
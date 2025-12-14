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
        'cell': Gauge('battery_cell', 'battery_cell',  labelnames=['meter', 'cell', 'metric'], registry=registry),
        'volts': Gauge('battery_volt', 'battery_volt',  labelnames=['meter'], registry=registry),
        'amps': Gauge('battery_amps', 'battery_amps',  labelnames=['meter'], registry=registry),
        'watts': Gauge('battery_watts', 'battery_watts',  labelnames=['meter'], registry=registry),
        'remain': Gauge('battery_remain', 'battery_remain',  labelnames=['meter'], registry=registry),
        'capacity': Gauge('battery_capacity', 'battery_capacity',  labelnames=['meter'], registry=registry),
        'cycles': Gauge('battery_cycles', 'battery_cycles',  labelnames=['meter'], registry=registry),
        'cellmin': Gauge('battery_cellmin', 'battery_cellmin',  labelnames=['meter'], registry=registry),
        'cellmax': Gauge('battery_cellmax', 'battery_cellmax',  labelnames=['meter'], registry=registry),
        'delta': Gauge('battery_delta', 'battery_delta',  labelnames=['meter'], registry=registry),
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

    print ("message2: " + json.dumps(message2))

def cellinfo3(data):
    # Process extended dd03 data (bytes 20-40 of the full message)
    # Example: 000024640310030b2e0b240b240000001388137a
    # Format: 00 00 | 24 64 | 03 10 | 03 | 0b 2e | 0b 24 | 0b 24 | 00 00 | 00 13 88 | 13 7a
    # Decoded: skip | protect | sensors+cells | sensors | temp1 | temp2 | temp3 | ...
    if len(data) < 14:
        return

    print(f"cellinfo3 raw data: {binascii.hexlify(data).decode('utf-8')}")

    try:
        # Skip first 2 bytes (00 00)
        # Bytes 2-3: protect status (2 bytes) - protection bits similar to cellinfo2
        # Byte 4: sensors count
        # Byte 5: cells count
        # Byte 6: sensors count (duplicate/confirmation?)
        # Bytes 7-12: three temperature values (2 bytes each)

        protect = struct.unpack_from('>H', data, 2)[0]
        cells = struct.unpack_from('>B', data, 5)[0]
        sensors = struct.unpack_from('>B', data, 6)[0]

        # Decode protection bits (same format as cellinfo2)
        prt = (format(protect, "b").zfill(16))

        print(f"Protection: {protect} (0x{protect:04x}), Cells: {cells}, Sensors: {sensors}")

        if sensors >= 3:
            temp1, temp2, temp3 = struct.unpack_from('>HHH', data, 7)

            # Sanity check - should be in Kelvin format (2700-3200 range for reasonable temps)
            if 2500 < temp1 < 3500 and 2500 < temp2 < 3500 and 2500 < temp3 < 3500:
                temp1_c = (temp1-2731)/10
                temp2_c = (temp2-2731)/10
                temp3_c = (temp3-2731)/10

                metrics['cell'].labels(meter, 'temp', 'temp1').set(temp1_c)
                metrics['cell'].labels(meter, 'temp', 'temp2').set(temp2_c)
                metrics['cell'].labels(meter, 'temp', 'temp3').set(temp3_c)

                print(f"Temperatures: {temp1_c:.1f}°C, {temp2_c:.1f}°C, {temp3_c:.1f}°C")

                # Add protection status metrics (same as cellinfo2)
                for prot_name, prot_bit in [
                    ('ovp', 0), ('uvp', 1), ('bov', 2), ('buv', 3),
                    ('cot', 4), ('cut', 5), ('dot', 6), ('dut', 7),
                    ('coc', 8), ('duc', 9), ('sc', 10), ('ic', 11), ('cnf', 12)
                ]:
                    metrics['cell'].labels(meter, 'protect', prot_name).set(int(prt[prot_bit:prot_bit+1]))
            else:
                print(f"Temperature values out of range: {temp1}, {temp2}, {temp3}")
    except Exception as e:
        print(f"cellinfo3 decode error: {e}")
        pass

def cellvolts1(data):            # process cell voltages
    global cells1
    celldata = data             # Unpack into variables, skipping header bytes 0-3
    i = 4
    cell1, cell2, cell3, cell4, cell5, cell6, cell7, cell8 = struct.unpack_from('>HHHHHHHH', celldata, i)
    cells1 = [cell1, cell2, cell3, cell4, cell5, cell6, cell7, cell8]     # needed for max, min, delta calculations
    message = {
        "meter" : "bms", 
        "cell1": cell1, 
        "cell2": cell2,
        "cell3": cell3, 
        "cell4": cell4,
        "cell5": cell5, 
        "cell6": cell6, 
        "cell7": cell7, 
        "cell8": cell8 
    }

    cell_volts = struct.unpack_from('>HHHHHHHH', celldata, i)
    for cell_id in range(8):
        metrics['cell'].labels(meter, str(f'{cell_id+1:02d}'), 'volts').set(cell_volts[cell_id])


def cellvolts2(data):            # process cell voltages
    celldata = data
    i = 0                       # Unpack into variables, ignore end of message byte '77'
    cell9, cell10, cell11, cell12, cell13, cell14, cell15, cell16,b77 = struct.unpack_from('>HHHHHHHHB', celldata, i)
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
    cell_volts = struct.unpack_from('>HHHHHHHHB', celldata, i)
    for cell_id in range(8):
        metrics['cell'].labels(meter, str(f'{cell_id+9:02d}'), 'volts').set(cell_volts[cell_id])

    cells2 = [cell9, cell10, cell11, cell12, cell13, cell14, cell15, cell16]    # adding cells min, max and delta values    
    allcells = cells1 + cells2
    cellsmin = min(allcells)
    cellsmax = max(allcells)
    delta = cellsmax-cellsmin
    mincell = (allcells.index(min(allcells))+1)                 # identify which cell # max and min
    maxcell = (allcells.index(max(allcells))+1)

    metrics['delta'].labels(meter).set(delta)
    metrics['cellmin'].labels(meter).set(mincell)
    metrics['cellmax'].labels(meter).set(maxcell)

class MyDelegate(DefaultDelegate):        # handles notification responses
    def __init__(self):
        DefaultDelegate.__init__(self)
        self.message_buffer = b''  # Buffer for multi-part BLE messages

    def handleNotification(self, cHandle, data):
        hex_data = binascii.hexlify(data)
        print (hex_data)
        text_string = hex_data.decode('utf-8')

        # Handle multi-part BLE messages (messages split across notifications)
        # BLE has 20-byte limit, so long messages are split into multiple parts
        if text_string.startswith('dd'):
            # Start of a new message - initialize buffer
            self.message_buffer = data
            # Check if this is a complete message (ends with 77)
            if text_string.endswith('77'):
                # Complete message, process immediately
                pass  # Fall through to routing logic
            else:
                # Incomplete, wait for continuation packets
                print(f'buffering message, waiting for continuation (buffer len: {len(self.message_buffer)} bytes)')
                return
        elif len(self.message_buffer) > 0:
            # We're buffering a multi-part message - append this packet
            print(f'appending to buffer (current buffer: {len(self.message_buffer)} bytes, adding: {len(data)} bytes)')
            self.message_buffer += data
            if not text_string.endswith('77'):
                # Still waiting for more packets
                print(f'still buffering (total buffer: {len(self.message_buffer)} bytes)')
                return
            # Message complete, use the full buffer
            print(f'message complete! total size: {len(self.message_buffer)} bytes')
            data = self.message_buffer
            hex_data = binascii.hexlify(data)
            text_string = hex_data.decode('utf-8')
            self.message_buffer = b''
        else:
            # Unexpected data with no buffered message - ignore or log
            if text_string == '00':
                # Single null byte - connection acknowledgment, ignore
                return
            print (f'unexpected packet (no active buffer): {text_string}')
            print (f'buffer length: {len(self.message_buffer)}')
            return

        # Route complete messages to decoding routines
        print(f'Routing message: starts with {text_string[:4]}, length {len(text_string)}, ends with {text_string[-4:]}')
        if text_string.find('dd04') != -1 and len(text_string) == 78:
            # Extended cell voltage message (20+19 bytes = 78 hex chars)
            print(f'Processing extended dd04 message ({len(text_string)} chars)')
            # Split into two parts and process
            cellvolts1(data[:20])  # First 20 bytes
            cellvolts2(data[20:])  # Remaining 19 bytes
        elif text_string.find('dd03') != -1 and len(text_string) == 90:
            # Extended pack info message (20+20+5 bytes = 90 hex chars)
            print(f'Processing extended dd03 message ({len(text_string)} chars)')
            # Split into parts and process
            cellinfo1(data[:20])   # First 20 bytes (standard pack info)
            cellinfo3(data[20:40]) # Next 20 bytes (extended sensor data)
            # The last 5 bytes don't contain enough data for cellinfo2
            # They appear to be a checksum or footer: 0000fae177
            # NOTE: cellinfo2 data (temps, protect, percent, fet) is NOT in this message!
        elif text_string.find('dd04') != -1:
            # Standard cell voltages part 1
            cellvolts1(data)
        elif text_string.find('dd03') != -1:
            # Standard pack info
            cellinfo1(data)
        elif text_string.find('77') != -1 and len(text_string) == 38:
            # Cell voltages part 2
            cellvolts2(data)
        elif text_string.find('77') != -1 and len(text_string) == 36:
            # Pack info part 2
            cellinfo2(data)
        else:
            print (f'unhandled complete message (len={len(text_string)}): {text_string}')
            print (f'unhandled complete message: {data}')

def connect ():
    try:
        print('attempting to connect')                        # connect bluetooth device
        bms = Peripheral(ble_device,addrType="public")
    except BTLEException as ex:
        print('cannot connect, sleep 10')
        time.sleep(10)
        return (None)
    else:
        print('connected ',ble_device)

    atexit.register(disconnect)
    bms.setDelegate(MyDelegate())        # setup bt delegate for notifications

    return (bms)

        # write empty data to 0x15 for notification request   --  address x03 handle for info & x04 handle for cell voltage
        # using waitForNotifications(5) as less than 5 seconds has caused some missed notifications
if __name__ == "__main__":
    bms = None
    while not bms:
        bms = connect ()

    start_http_server(port,registry=registry)

    while True:
        try:
            result = bms.writeCharacteristic(0x15,b'\xdd\xa5\x04\x00\xff\xfc\x77',False)        # write x04 w/o response cell voltages
            bms.waitForNotifications(5)
            result = bms.writeCharacteristic(0x15,b'\xdd\xa5\x03\x00\xff\xfd\x77',False)        # write x03 w/o response cell info
            bms.waitForNotifications(5)
            time.sleep(z)
        except BTLEException as ex:
            print(f'BLE disconnected: {ex}')
            print('Attempting to reconnect...')
            bms = None
            while not bms:
                bms = connect()
                if not bms:
                    time.sleep(5)

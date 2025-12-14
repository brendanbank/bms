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
    def handleNotification(self, cHandle, data):
        hex_data = binascii.hexlify(data)
        text_string = hex_data.decode('utf-8')
        if text_string.find('dd04') != -1:        # check incoming data for routing to decoding routines
            cellvolts1(data)
        elif text_string.find('dd03') != -1:
            cellinfo1(data)
        elif text_string.find('77') != -1 and len(text_string) == 38:     # x04
            cellvolts2(data)
        elif text_string.find('77') != -1 and len(text_string) == 36:     # x03
            cellinfo2(data)        
#        else:
#            print (f'unknown: {text_string}')
#            print (f'unknown: {data}')

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
        result = bms.writeCharacteristic(0x15,b'\xdd\xa5\x04\x00\xff\xfc\x77',False)        # write x04 w/o response cell voltages
        bms.waitForNotifications(5)
        result = bms.writeCharacteristic(0x15,b'\xdd\xa5\x03\x00\xff\xfd\x77',False)        # write x03 w/o response cell info
        bms.waitForNotifications(5)
        time.sleep(z)

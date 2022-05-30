import base64
import json
import os
import paho.mqtt.client as mqtt
import sanic
from sanic import Sanic
import sys
import logging
import base64
import struct
from sanic.log import logger as log
from math import pi, sqrt, sin, cos, atan2

import sqlite3 as sldb

APPNAME='WPMC'

tablename1='DateWPMC_app1'
tablename2='DateWPMC_app2'
tablename3='DateWPMC_app3'
bazadedate='WPMCacasa.db'

#MQTT_HOST = '192.168.241.21'
# MQTT_HOST='127.0.0.1'
MQTT_HOST='mqtt.beia-telemetrie.ro'
MQTT_PORT = 1883

MQTT_TOPIC_PREFIX = 'smartdelta-lora-test'
MQTT_TOPIC_PREFIX = 'lora/ttn/beia-libelium/PyTest'

MY_GW = 'pygate-ana2809'

app = Sanic(APPNAME)


sir1="f"*12
_LORA_PKG_FORMAT1 ="!"+sir1

sir2="f"*2+'H'
_LORA_PKG_FORMAT2 ="!"+sir2

sir3="f"*3
_LORA_PKG_FORMAT3 ="!"+sir3

parametri_gen=["appid", "devid", "gwid", "gweui", "timestamp", "snr", "rssi", "received",
           "bw", "sf", "cr", "freq"]
parametri1=["altitudine","presiune","temperatura", "umiditate", "punct_roua",
           "Lumina_B","Lumina_R","acceleratiex","acceleratiey","acceleratiez","roll","pitch"]
app1_id='chap7'

parametri2=["latitudine", "longitudine", "distanta"]
app2_id='smartdelta-app2'

parametri3=["nivel apa", "precipitatii", "lumina vizibila"]
app3_id='smartdelta-app3'



############# Functions##############

def calcul_dist(lat,lon):
    radius = 6371 * pow(10,3)
    lat1 = lat * pi / 180
    lon1 = lon * pi / 180
    latG = 44.39554977416992* pi / 180
    lonG = 26.102664947509766* pi / 180
    deltaLat = lat1 - latG
    deltaLon = lon1 - lonG
    a = pow(sin(deltaLat/2), 2) + cos(lat1) * cos(lat1) * pow(sin(deltaLon / 2),2)
    cc = 2 * atan2(sqrt(a), sqrt(1 - a))
    d1m = radius * cc
    return d1m


async def overtake_metadata(request):
    APP_ID = request.json['end_device_ids']['application_ids']['application_id']
    DEVICE_ID = request.json['end_device_ids']['device_id']
    print(APP_ID)
    # chestii relevante (RSSI, ID gateway...) -- nu prea multe
    # METADATE = request.json['metadata']
    receivedat = request.json['received_at']
    UPLINKMSG = request.json['uplink_message']
    print(UPLINKMSG)
    SETTINGS= UPLINKMSG['settings']
    DATARATE=SETTINGS['data_rate']
    LORA=DATARATE['lora']
    bw=LORA['bandwidth']
    sf=LORA['spreading_factor']
    cr=SETTINGS['coding_rate']
    freq=SETTINGS['frequency']
    TIMEONAIR=UPLINKMSG['consumed_airtime']
    print(TIMEONAIR)
    METADATE = UPLINKMSG['rx_metadata']
    l_META=len(METADATE)
    for i in range(l_META):
        GATEWAY=METADATE[i]
        GATEWAYID=GATEWAY['gateway_ids']['gateway_id']
        GATEWAYIDs = GATEWAY['gateway_ids']
        chei_gw = GATEWAYIDs.keys()
        print(chei_gw)
        if "eui" in chei_gw:
            GATEWAYEUI = GATEWAY['gateway_ids']['eui']
        else:
            GATEWAYEUI = "noeui"
        chei_gw2 = GATEWAY.keys()
        if "time" in chei_gw2:
            timestamp = GATEWAY['time']
        else:
            timestamp = request.json['received_at']
        rssi = GATEWAY['rssi']
        snr = GATEWAY['snr']
        print(type(snr))
        print(type(rssi))
        print(type(GATEWAYID))
        if GATEWAYID == MY_GW:
            print('Parameters of our gateway are: SNR: {}, RSSI: {}, gateway id: {}:'.format(snr, rssi, GATEWAYID))
    payloadbytes = UPLINKMSG['decoded_payload']['bytes']
    print(payloadbytes)
    ab = bytearray(payloadbytes)
    ls_gen=[APP_ID, DEVICE_ID, GATEWAYID, GATEWAYEUI, timestamp, snr,rssi, receivedat, bw, sf, cr, freq]
    if APP_ID == app1_id:
        altitudine, presiune, temperatura, umiditate, punct_roua, B_lux, R_lux, xacc, yacc, zacc, rolll, pitchh = struct.unpack(
            _LORA_PKG_FORMAT1, ab)
        print("Altitude:", altitudine)
        print("Atmospheric pressure", presiune)
        print("Temperature:", temperatura)
        print("Humidity:", umiditate)
        print("Dew Point:", punct_roua)
        print("Light, Blue channel:", B_lux)
        print("Light, Red channel:", R_lux)
        print("Acceleration x-axis:", xacc)
        print("Acceleration y-axis:", yacc)
        print("Acceleration z-axis:", zacc)
        print("Roll", rolll)
        print("Pitch", pitchh)
        ls=ls_gen+[altitudine, presiune, temperatura, umiditate, punct_roua, B_lux, R_lux,xacc, yacc, zacc, rolll, pitchh]
    elif APP_ID==app2_id:
        latitudine,longitudine, ID=struct.unpack(_LORA_PKG_FORMAT2, ab)
        print("Latitude", latitudine)
        print("Longitude", longitudine)
        dist = calcul_dist(latitudine, longitudine)
        print("The distance is:", dist)
        ls = ls_gen + [latitudine, longitudine,dist]
        print("Last message has the ID", ID)

    elif APP_ID==app3_id:
        nivel_apa, precipitatii, lumina_vizibila=struct.unpack(_LORA_PKG_FORMAT3, ab)
        print("Water level", nivel_apa)
        print("Rainfall", precipitatii)
        print("Visible light", lumina_vizibila)
        ls = ls_gen + [nivel_apa, precipitatii, lumina_vizibila]
    return ls


def createtable(appid):
    with con:
        if appid==app1_id:
            con.execute("""
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                appid TEXT,
                devid TEXT,
                gwid TEXT,
                gweui TEXT,
                timestamp INTEGER,
                snr REAL,
                rssi REAL,
                received TEXT,
                bw REAL,
                sf INTEGER, 
                cr TEXT,
                freq REAL,
                altitude REAL,
                pressure REAL,
                temperature REAL,
                humidity REAL,
                dew_point REAL,
                light_B REAL,
                light_R REAL,
                accelerationx REAL,
                accelerationy REAL,
                accelerationz REAL,
                roll REAL,
                pitch REAL
            );
        """.format(tablename1))
        elif appid == app2_id:
                con.execute("""
                CREATE TABLE IF NOT EXISTS {} (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    appid TEXT,
                    devid TEXT,
                    gwid TEXT,
                    gweui TEXT,
                    timestamp INTEGER,
                    snr REAL,
                    rssi REAL,
                    received TEXT,
                    bw REAL,
                    sf INTEGER, 
                    cr TEXT,
                    freq REAL,
                    latitude REAL,
                    longitude REAL,
                    distance REAL
                );
            """.format(tablename2))
        elif appid == app3_id:
            con.execute("""
                CREATE TABLE IF NOT EXISTS {} (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    appid TEXT,
                    devid TEXT,
                    gwid TEXT,
                    gweui TEXT,
                    timestamp INTEGER,
                    snr REAL,
                    rssi REAL,
                    received TEXT,
                    bw REAL,
                    sf INTEGER, 
                    cr TEXT,
                    freq REAL,
                    waterlevel REAL,
                    rainfall REAL,
                    visible light REAL
                );
            """.format(tablename3))
    return 0

def select_all_tasks(con,appid):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    if appid==app1_id:
        tablename=tablename1
    elif appid==app2_id:
        tablename = tablename2
    elif appid == app3_id:
        tablename = tablename3
    cur = con.cursor()
    cur.execute("SELECT * FROM {}".format(tablename))

    rows = cur.fetchall()

    for row in rows:
        print(row)

async def appendentry(ls):
    appid=ls[0]
    if appid==app1_id:
        tablename=tablename1
        formatare="?,"*23
        formatare="("+formatare+"?) ;"
        sir_f="INSERT INTO {} (appid, devid, gwid, gweui, timestamp, snr, rssi, received, bw, sf, cr, freq, altitude,pressure,temperature, humidity, dew_point,light_B,light_R,accelerationx,accelerationy,accelerationz,roll,pitch) VALUES "+formatare
    elif appid==app2_id:
        tablename = tablename2
        formatare="?,"*14
        formatare="("+formatare+"?) ;"
        sir_f="INSERT INTO {} (appid, devid, gwid, gweui, timestamp, snr, rssi, received, bw, sf, cr, freq, latitude, longitude, distance) VALUES "+formatare
    elif appid==app3_id:
        tablename = tablename3
        formatare="?,"*14
        formatare="("+formatare+"?) ;"
        sir_f="INSERT INTO {} (appid, devid, gwid, gweui, timestamp, snr, rssi, received, bw, sf, cr, freq, nivel-apa, precipitatii, lumina-vizibila) VALUES "+formatare

    sql = sir_f.format(tablename)
    # data = (ls)
    data=tuple(ls)
    cursor=con.cursor()
    cursor.execute(sql,data)
    con.commit()
    print('Insertion ok')
    cursor.close()
    return 0

async def send_mqtt(ls):
    APP_ID = ls[0]
    device_id = ls[1]
    GATEWAYID=ls[2]
    mqtt_topic = '{prefix}/{app_id}/{device_id}'.format(prefix=MQTT_TOPIC_PREFIX, app_id=APP_ID, device_id=device_id)
    mqtt_topic2 = '{prefix}/{gateway}'.format(prefix=MQTT_TOPIC_PREFIX, gateway=GATEWAYID)
    d = {}
    if APP_ID==app1_id:
        if len(parametri1)+len(parametri_gen) == len(ls):
            param=parametri_gen+parametri1
            print("The number of parameters is correct")
        else:
            print("The number of parameters is not equal to the number of values!")
    elif APP_ID==app2_id:
        if len(parametri2)+len(parametri_gen) == len(ls):
            print("The number of parameters is correct")
            param=parametri_gen+parametri2
        else:
            print("The number of parameters is not equal to the number of values!")
    elif APP_ID==app3_id:
        if len(parametri3)+len(parametri_gen) == len(ls):
            print("The number of parameters is correct")
            param=parametri_gen+parametri3
        else:
            print("The number of parameters is not equal to the number of values!")
    for i in range(len(param)):
                d[param[i]] = str(ls[i])
    mqtt_payload = json.dumps(d)
    print(mqtt_payload, type(mqtt_topic))
    app.mqtt_client.publish(mqtt_topic, mqtt_payload, qos=2)
    app.mqtt_client.publish(mqtt_topic2, mqtt_payload, qos=2)
    print("The payload was sent")
    print("MQTT topic:", mqtt_topic)


@app.post('/')
async def test(request):
    print(request)

    while (True):
        ls = await overtake_metadata(request)
        appid=ls[0]
        await appendentry(ls)
        # select_all_tasks(con, appid)
        await send_mqtt(ls)
        return sanic.response.json({'status': 'ok'})
    # else:
    #     log.error('Incorrect Authorization Header!')
    #     return sanic.response.json({'status': 'invalid authorization header'})


if __name__ == '__main__':
    print('//////')
    con = sldb.connect(bazadedate)
    createtable(app1_id)
    createtable(app2_id)
    createtable(app3_id)

    app.mqtt_client = mqtt.Client()
    app.mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    log.info ("Running with MQTT HOST %s and MQTT PORT %s", MQTT_HOST, MQTT_PORT)
    app.mqtt_client.loop_start()
    app.run(debug=True, access_log=False)

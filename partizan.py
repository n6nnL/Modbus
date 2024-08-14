import time
from datetime import datetime
import os
from pymodbus.client import ModbusSerialClient as ModbusClient
import json
import requests
import csv
import pytest



class Demo:
    def __init__(self):
        global client
        self.PV1_voltage = 0
        self.PV1_current = 0
        self.PV2_voltage = 0
        self.PV2_current = 0
        self.PV3_voltage = 0
        self.PV3_current = 0
        self.PV4_voltage = 0
        self.PV4_current = 0
        self.a_phase_voltage = 0
        self.b_phase_voltage = 0
        self.c_phase_voltage = 0
        self.a_phase_current = 0
        self.b_phase_current = 0
        self.c_phase_current = 0
        self.line_voltage_ab = 0
        self.line_voltage_bc = 0
        self.line_voltage_ca = 0
        self.temperature = 0
        self.power_factor = 0
        self.status_inv = 0
        self.grid_frequency = 0
        self.total_active_power = 0
        self.reactive_power = 0
        self.daily_energy = 0
        self.month_energy = 0
        self.states = 0
        self.temperature = 0
        self.ac_voltage = 0
        self.ac_current = 0

    def bit16to32(self, a, b):
        num1 = format(a, '016b') + format(b, '016b')
        num = int(num1, 2)
        return num

    def unsigned_int16(self, a):
        num1 = format(a, '016b')
        if num1[0] == '0':
            num = int(num1, 2)
        else:
            num = -32768 + int(num1[1:16], 2)
        return num

    def unsigned_int32(self, a):
        num1 = format(a, '032b')
        if num1[0] == '0':
            num = int(num1, 2)
        else:
            num = -2147483648 + int(num1[1:32], 2)
        return num

    def get_data(self, slave):
        self.PV1_voltage, self.PV2_voltage,  = (
            client.read_input_registers(address=311, count=2, slave=slave)).registers
        self.PV2_current, self.PV1_current = (
            client.read_input_registers(address=323, count=2, slave=slave)).registers
        
        self.a_phase_voltage, self.b_phase_voltage, self.c_phase_voltage, self.a_phase_current, self.b_phase_current, self.c_phase_current = (
            client.read_input_registers(address=627, count=6, slave=slave)).registers
        total_act_power_upper, total_act_power_lower, reactive_power_upper, reactive_power_lower, self.power_factor = (
            client.read_input_registers(address=504, count=5, slave=slave)).registers
        self.power_factor = client.read_input_registers(address=621, count=5, slave=slave).registers[0]
        self.total_active_power = self.bit16to32(total_act_power_lower, total_act_power_upper)
        self.grid_frequency = (client.read_input_registers(address=183, count=1, slave=slave)).registers[0]
        self.reactive_power = self.bit16to32(reactive_power_lower, reactive_power_upper)
        self.temperature = (client.read_input_registers(address=540, count=1, slave=slave)).registers[0]
        self.daily_energy = (client.read_input_registers(address=500, count=1, slave=slave)).registers[0]
        self.states = (client.read_input_registers(address=551, count=1, slave=slave)).registers[0]

    def process(self):
        self.PV1_voltage = self.PV1_voltage / 10
        self.PV1_current = self.PV1_current / 10
        self.PV2_voltage = self.PV2_voltage / 10
        self.PV2_current = self.PV2_current / 10
        self.temperature = self.temperature / 10
        self.power_factor = self.unsigned_int16(self.power_factor) / 1000
        self.grid_frequency = self.grid_frequency / 10
        self.line_voltage_ca = self.line_voltage_ca / 10
        self.line_voltage_bc = self.line_voltage_bc / 10
        self.line_voltage_ab = self.line_voltage_ab / 10
        self.a_phase_voltage = self.a_phase_voltage / 10
        self.b_phase_voltage = self.b_phase_voltage / 10
        self.c_phase_voltage = self.c_phase_voltage / 10
        self.a_phase_current = self.a_phase_current / 10
        self.b_phase_current = self.b_phase_current / 10
        self.c_phase_current = self.c_phase_current / 10
        self.total_active_power = self.total_active_power / 1000
        self.reactive_power = self.unsigned_int32(self.reactive_power) / 1000
        self.daily_energy = self.daily_energy / 10
        self.ac_voltage = float(self.a_phase_voltage + self.b_phase_voltage + self.c_phase_voltage) / 3
        self.ac_current = float(self.a_phase_current + self.b_phase_current + self.c_phase_current) / 3
        self.status_code = 0

    def print_values(self):
        print(f'''{self.PV1_voltage} 
                    voltage1: {self.PV1_current} 
                    a_phase_voltage: {self.a_phase_voltage}
                    b_phase_voltage: {self.b_phase_voltage}
                    active_power : {self.total_active_power}
                    status: {self.status_inv}
                    daily_energy : {self.daily_energy}
                    monthly energy : {self.month_energy}
                    states : {format(self.states, '016b')}
                    ''')


inverter1 = Demo()
inverter2 = Demo()

class Postgres:
    def __init__(self):
        self.infos = "https://monitoring.g-power.mn/api/infos"
        self.inverterdc = "https://monitoring.g-power.mn/api/inverter-dc/"
        self.inverters = "https://monitoring.g-power.mn/api/inverters/"
        self.con_post = False
        self.now = datetime.now()
        self.token = {
            'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwianRpIjoiZmM5MzU4NTcxZDhiMzE3Yzg4YjNmNGJmYTc4MGI4ZDlkZTIwNWE2NmI0ZDViZDEyYWEyN2FiZGE1YjM0ZDE0N2RlNTk0NzIxOTYxNGIxYzAiLCJpYXQiOjE2NjMzMTU0NjAuNDgwNjcxLCJuYmYiOjE2NjMzMTU0NjAuNDgwNjc1LCJleHAiOjE2Nzg5NTM4NjAuNDUyNDgyLCJzdWIiOiIxOSIsInNjb3BlcyI6W119.OyiWFwHnbEkKYPGVzM_yWH2Z3C9cdDIgIyz1dTcZ4xg4JpgX-U6-cuSmXiwRJXm5YxFeT4FQ35OJ-ZMBpPxYjVlGgBTrwQnR9ABV2Te8H-ub_RD7QHE0HX8fnlDihXdpBkgiqQiQQckwE6c7x5i50e1tqcjctffURwSQhdS4OmDmuDi7-m7kB9R0v77Vk7pt1aJ9MlofsoqFMkPRLCZv7nSCbq59e_CCZ3N07pHxNT_hnslVpIabYG9z0jRRa6rlCjWpOftguuanspqNvYQYk0EiNuthFOVfcUoMizkAUU1I7ZxOO5OgwWbyIZL0Q1SagXAzLnLerxw3HC3jfLXPoBjm4GpauW7v_2AW5nhjU3j-XWi3j7iB01Cu4161sLmbYqWH5EaWBa5ZzmmXHGM_diIvyicoGDkq8rBmEH8xNJql5954NK1rGpJ2m8kwMGFsdKR4JhptPMI86OOvCQjxGv4SzY5SzoaNUjOsDb17o3iVqTC7Ew1z_IXlWTPlrsO0f0B5uWlo21H3ABdfwNMDcSgxg2kiCxxjzcSVql2yzNUhjSPCHnxqQg1cKkzim7X91y28J5k8bTojc9dTUp93CoHjhvbHjrrQbJzIepIO5bcfkQkYECdw7DCQFnuOU2qfiQFi7_Wi3aumpcnKvHs5DsAx0juW-YAW52gVM7Yl_Vg',
            'Content-Type': 'application/json'
        }
        self.date = "%Y-%m-%d %H:%M:%S"
    def connect_post(self):
        try:
            response = requests.get("https://monitoring.g-power.mn/")
            if (response.status_code == 200):
                return True
            else:
                return False
        except:
            return False

    def send_off(self, i):
        with open('off_data.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                if (row != []):
                    print(requests.request("POST", self.inverters+i, headers=self.token, json={
                                                                                    "client_date": row[0],
                                                                                    "power": row[1],
                                                                                    "energy": row[2],
                                                                                    "phase_voltage_van": row[3],
                                                                                    "phase_voltage_vbn": row[4],
                                                                                    "phase_voltage_vcn": row[5],
                                                                                    "phase_current_ian": row[6],
                                                                                    "phase_current_ibn": row[7],
                                                                                    "phase_current_icn": row[8],
                                                                                    "power_factor_avg": row[9],
                                                                                    "reactive_avg_power_q": row[10],
                                                                                    "temperature": row[11],
                                                                                    "system_status": row[12]
                                                                                }).content)
                    print(requests.request("POST", self.inverterdc + i, headers=self.token, json={
                                                                                    "client_date": row[0],
                                                                                    "ac_voltage": row[13],
                                                                                    "ac_current": row[14],
                                                                                    "voltage_1": row[15],
                                                                                    "current_1": row[16],
                                                                                    "voltage_2": row[17],
                                                                                    "current_2": row[18]
                                                                                }).content)
                    time.sleep(2)
        with open('off_data.csv', mode='w') as off_data:
            employee_writer = csv.writer(off_data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([])

    def process(self, inverter, timer):
        self.inverterdc_data = {
            "client_date": timer,
            "ac_voltage": inverter.c_phase_voltage,
            "ac_current": inverter.c_phase_voltage,
            "voltage_1": inverter.PV1_voltage,
            "current_1": inverter.PV1_current,
            "voltage_2": inverter.PV2_voltage,
            "current_2": inverter.PV2_current
        }
        self.inverters_data = {
            "client_date": timer,
            "power": inverter.total_active_power,
            "energy": inverter.daily_energy,
            "phase_voltage_van": inverter.a_phase_voltage,
            "phase_voltage_vbn": inverter.b_phase_voltage,
            "phase_voltage_vcn": inverter.c_phase_voltage,
            "phase_current_ian": inverter.a_phase_current,
            "phase_current_ibn": inverter.b_phase_current,
            "phase_current_icn": inverter.c_phase_current,
            "power_factor_avg": inverter.power_factor,
            "reactive_avg_power_q": inverter.reactive_power,
            "temperature": inverter.temperature,
            "system_status": inverter.status_inv
        }

    def off_work(self, inverter,timer):
        with open('off_data.csv', mode='a') as off_data:
            employee_writer = csv.writer(off_data)
            employee_writer.writerow([timer,
                                      inverter.total_active_power,
                                      inverter.daily_energy,
                                      inverter.a_phase_voltage,
                                      inverter.b_phase_voltage,
                                      inverter.c_phase_voltage,
                                      inverter.a_phase_current,
                                      inverter.b_phase_current,
                                      inverter.c_phase_current,
                                      inverter.power_factor,
                                      inverter.reactive_power,
                                      inverter.temperature,
                                      inverter.status_inv,
                                      inverter.ac_voltage,
                                      inverter.ac_current,
                                      inverter.PV1_voltage,
                                      inverter.PV1_current,
                                      inverter.PV2_voltage,
                                      inverter.PV2_current])

    def send_power(self, power, timer):
        requests.request("POST", self.infos, headers=self.token, json={
            "client_date": timer,
            "grid_from_energy": power[4],
            "grid_to_energy": power[1],
            "grid_power": power[3],
            "load_power": power[2],
            "load_energy" : power[0],
                    })

    def send_data(self, i):
        requests.request("POST", self.inverterdc + i, headers=self.token, json=self.inverterdc_data)
        requests.request("POST", self.inverters + i, headers=self.token, json=self.inverters_data)

def unsigned_int16(a):
    num1 = format(a, '016b')
    if num1[0] == '0':
        num = int(num1, 2)
    else:
        num = -32768 + int(num1[1:16], 2)
    return num

def check_con_inv():
    global client
    try:
        a = client.read_holding_registers(address=500, count=1, slave=1).registers
        return True
    except:
        return False


def user_process():
    pass

def cal_power(timer):
    global power
    with open('power_daily.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if row != []:
                time = row[0]
                if time[9] == timer[9]:
                    power[0] = power[0] - float(row[1])
                    power[1] = power[1] - float(row[2])
                    power[5] = power[5] * 277.7 - float(row[3])
                    print(power)
                else:
                    with open('power_daily.csv', mode='w') as csv_file_write:
                        employee_writer = csv.writer(csv_file_write, delimiter=',', quotechar='"',
                                                        quoting=csv.QUOTE_MINIMAL)
                        employee_writer.writerow([timer,power[0],power[1],power[5] * 277.7])



def power_meter():
    global power
    power29_r = int(client.read_input_registers(address=29, count=1, slave=6).registers[0])
    power30_r = int(client.read_input_registers(address=30, count=1, slave=6).registers[0])
    power31_r = int(client.read_input_registers(address=31, count=1, slave=6).registers[0])
    power32_r = int(client.read_input_registers(address=32, count=1, slave=6).registers[0])
    power29_l = int(client.read_input_registers(address=29, count=1, slave=7).registers[0])
    power30_l = int(client.read_input_registers(address=30, count=1, slave=7).registers[0])
    power31_l = int(client.read_input_registers(address=31, count=1, slave=7).registers[0])
    power32_l = int(client.read_input_registers(address=32, count=1, slave=7).registers[0])
    power[0] = (65536*power29_r+power30_r) * 500 / 1000 + (65536*power29_l+power30_l) * 500 / 1000
    power[1] = (65536*power31_r+power32_r) * 500 / 1000 + (65536*power31_l+power32_l) * 500 / 1000
    power[2] = client.read_input_registers(address=7, count=1, slave=6).registers[0] + client.read_input_registers(address=7, count=1, slave=7).registers[0]
    power[2] = unsigned_int16(power[2]) * 50 / 1000
    print(power)
    return power

#def find_usb():
    #try:
       # client = ModbusClient(method='rtu', port='/dev/ttyUSB0', stopbit=1, bytesize=8, parity='N', baudrate=9600,
                              #timeout=0.5)
        #client.connect()
        #a = client.read_input_registers(address=29, count=1, slave=6).registers
        #return '0'
    #except:
        #client = ModbusClient(method='rtu', port='/dev/ttyUSB1', stopbit=1, bytesize=8, parity='N', baudrate=9600,
                            #  timeout=0.5)
       # client.connect()
        #a = client.read_input_registers(address=29, count=1, slave=6).registers
        #return '1'
def power_process(power):
    power[4] = power[0] - inverter_energy
    if power[4] > 0 :
        power[4] = power[4]
    else:
        power[4] = 0
    power[1] = inverter_energy - power[0]
    if power[1] > 0 :
        power[1] = power[1]
    else:
        power[1] = 0
    power[3] = ((inverter1.total_active_power) - power[2]) * (-1)
    return power

post1 = Postgres()
post2 = Postgres()
start_con = time.time()
start_get = time.time()
total_time = 0
num = '0'
num1 = '0'
usb = '/dev/ttyUSB'
power = [0, 0, 0, 0, 0, 0, 0]
#try:
   # num = find_usb()
   # if num == '1':
   #     num1 = '0'
   # else:
   #     num1 = '1'
#except:
    #os.system('python3 dpp.py')
try:
    while True:
        end_con = time.time()
        end_get = time.time()
        if (end_con - start_con) > 5:
            client = ModbusClient(method='rtu', port=usb+ num, stopbit=1, bytesize=8, parity='N', baudrate=9600,timeout=0.5)
            con_inv = client.connect()
            print(con_inv)
            if (end_get - start_get) > 40:
                print('5 connected start')
                now = datetime.now()
                timer = now.strftime("%Y-%m-%d %H:%M:%S")
                power = power_meter()
                cal_power(timer)
                if check_con_inv() == True:
                    start_time = time.time()
                    print('10 get_data start')
                    time.sleep(1)
                    print(power)
                    inverter1.get_data(1)
                    inverter1.process()
                    time.sleep(1)
                    inverter2.get_data(2)
                    inverter2.process()
                    print(inverter1.daily_energy)
                    time.sleep(1)
                    print(inverter1.total_active_power)
                    post1.process(inverter1, timer)
                    post2.process(inverter2, timer)
                    inverter_energy = inverter1.daily_energy 
                    with open('inverter_energy.csv', mode='w') as csv_file_write2:
                        employee_writer = csv.writer(csv_file_write2, delimiter=',', quotechar='"',
                                                        quoting=csv.QUOTE_MINIMAL)
                        employee_writer.writerow([timer,inverter_energy])
                    power = power_process(power)
                    if post1.connect_post() == True:
                        # post.send_off('7')
                        post1.send_data('82')
                        print('1 worked')
                    else:
                        # post.off_work(inverter1,timer)
                        print('post1 not working')
                    if post2.connect_post() == True:
                        # post.send_off('7')
                        post2.send_data('84')
                        print('2 worked')
                    else:
                        # post.off_work(inverter1,timer)
                        print('post2 not working')
                else:
                    print('connection lost: connect inverter')
                    with open('inverter_energy.csv', mode='r') as csv_file:
                        csv_reader = csv.reader(csv_file, delimiter=',')
                        for row in csv_reader:
                            if row != []:
                                time_inv = row[0]
                                if timer[9] == time_inv[9]:
                                    power[4] = float(row[1]) + power[0] - power[1]
                                else:
                                    with open('inverter_energy.csv', mode='w') as csv_file_write:
                                        employee_writer = csv.writer(csv_file_write, delimiter=',', quotechar='"',
                                                                        quoting=csv.QUOTE_MINIMAL)
                                        employee_writer.writerow([timer,0])
                    power[3] = power[2]
                post1.send_power(power, timer)
                start_get = time.time()
            client.close()
            start_con = time.time()
except:
    print('systemd ymar negen aldaa garlaa')
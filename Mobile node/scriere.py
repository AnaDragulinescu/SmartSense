def init_scriere(writepath):
    with open (writepath, 'a') as datafile:
        datafile.write("IDmesaj, An,Luna,Zi, ora, minut, secunda, ms, x, age, latitudine, longitudine, ttf, Mesaj, RSSI, SNR, SFRX, SFTX, TX_trials, TX_power, TX_time_on_air, TX_counter, TX_frequency")
def Scriere_param(writepath,rtc,l76,lora,IDmsg):
    with open (writepath, 'a') as datafile:
        datafile.write(str(IDmsg))
        datafile.write(",")
        datafile.write(str(rtc.now()))
        datafile.write(",")
        datafile.write(str(l76.coordinates()))
        datafile.write("\n")
        print('Scris in fisier.')

def Scriere_param2(writepath,rtc,l76):
    with open (writepath, 'a') as datafile:
        datafile.write("An,Luna,Zi, ora, minut, secunda, ms, x, age, latitudine, longitudine, ttf, Altitudine, RSSI, SNR, SFRX, SFTX, TX_trials, TX_power, TX_time_on_air, TX_counter, TX_frequency")
        datafile.write(",")
        datafile.write(str(rtc.now()))
        datafile.write(",")
        datafile.write(l76.coordinates())
        datafile.write(",")
        datafile.write("Mesaj gol")
        datafile.write("\n")
        print('Scris in fisier.')

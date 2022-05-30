from network import LoRa
import binascii
print(binascii.hexlify(LoRa().mac()).upper())

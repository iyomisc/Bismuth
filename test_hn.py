from socket import socket
import connections

s = socket()
s.settimeout(10)
s.connect(("127.0.0.1", 5658))

connections.send (s, "HN_test")
test1 = connections.receive (s)
print("HN_test", test1)

connections.send (s, "HN_reg_check_weight 36c59c7780546179d7f80729e1ccc3932260b849bc519b2120074169 1195000")
test2 = connections.receive (s)
print("HN_reg_check_weight", test2)

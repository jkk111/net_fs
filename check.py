from sys import argv

f_name = argv[1]
seek = int(argv[2])
read = int(argv[3])

f = open(f_name, 'rb')
f.seek(seek)

buf = f.read(read)

print(buf)

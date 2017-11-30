from sys import argv

opts={}
print (argv)
while argv:
    if (argv[0][0]=="-"):
        opts[str(argv[0]).replace("-","")] = argv[1]
    argv = argv[1:]
print(opts)

    
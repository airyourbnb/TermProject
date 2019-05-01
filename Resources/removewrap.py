with open("sortedAV",newline='') as f1:
    lines = f1.readlines()
    for line in lines:
        print(line.replace(",WrappedArray", "\t\t"))
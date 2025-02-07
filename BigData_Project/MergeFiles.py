import os

def merge_files():
    outputDir = "/home/sunbeam/Desktop/BigData_Project/Extra"
    outputFile = "/home/sunbeam/Desktop/BigData_Project/Extra/MergedStockData.json"
    partfiles=[f for f in os.listdir(outputDir) if f.startswith('part-')]

    with open(outputFile,'w') as outfile:
        for partfile in partfiles:
            abspath=os.path.join(outputDir,partfile)

            with open(abspath,'r') as file:
                for line in file:
                    outfile.write(line)
            os.remove(abspath)
    print("Merged all files")
merge_files()
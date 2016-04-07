#!/usr/local/apps/anaconda/3-2.2.0/bin/python

# Local realignment around indels

# With some minor arguments, this compiles the command and runs it multithreaded, 1 for
# each of the files you send it

# import sys, os, threading
# from functools import reduce

import sys
import multiprocessing as mp
from time import strftime

import baseFuns as lan

__author__ = 'Lucas Nell'




###############################
# EXAMPLE DICTIONARY (for 'Command' below)
###############################
# Dictionary = {
#     'input': 'castH12_X9mD.bam',  # input bam file; will change for each thread
#     'ass': '/lustre1/lan/MouseGenome/chr19_mm9.fa',  # Assembly fasta file
#     'dir': '/lustre1/lan/cast/BAM_X9'  # Directory for bam files
# }


###############################
# Command
###############################

Command = \
'''export file=%(input)s
export assembly=%(ass)s\n

module load java/jdk1.7.0_67\n

cd %(dir)s\n

java -Xmx8g -jar /usr/local/apps/picard/1.135/dist/picard.jar MarkDuplicates \\
CREATE_INDEX=true \\
INPUT=${file} \\
OUTPUT=${file/P.bam/mD.bam} \\
MAX_RECORDS_IN_RAM=500000 \\
TMP_DIR=$(dir)s \\
METRICS_FILE=${file/P.bam/mD.txt}
'''


###############################
# Read command line args
###############################

# From the ID input from bash script, get 5 main objects for further functions
Assembly, Subspp, Suff, Directory, Dict = lan.GetObjs(sys.argv[1])

# File search for bam files
bamFiles = lan.GetBAMs(Directory, Subspp, Suff + 'P.bam')




###############################
# Running commands
# on separate threads
###############################

if __name__ == '__main__':
    print('~'*25, '\nmarkDups.py\nStarted', strftime("%D at %H:%M:%S"), '\n', '. '*3)
    threads = []
    for b in bamFiles:
        t = mp.Process(target = lan.DoComm, args = (b, Command, Dict))
        threads.append(t)
    # Start the processes
    for t in threads:
        t.start()
    # Ensure all of the processes have finished
    for t in threads:
        t.join()
    print('Process Ended', strftime("%D at %H:%M:%S"), '\n' + '~'*25)



# os.system('''scp /Users/lucasnell/uga/Python/ReadsBySample/markDups.py \
# lan@xfer2.gacrc.uga.edu:~/GATKpipe''')





# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # for testing
#
# from imp import reload
# reload(lan)
# Assembly, Subspp, Suff, Directory, Dict = lan.GetObjs('cast_X9')#sys.argv[1])
#
# print(Assembly, Subspp, Suff, Directory, Dict, sep='\n')
#
# bamFiles = lan.FindFiles(Directory, Subspp, Suff + 'P.bam')
#
#
# c = Dict
# c['input'] = bamFiles[0]
#
# NewComm = str(Command % c)
# print(NewComm)
#
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Call variants with HaplotypeCaller.

With some minor arguments, this compiles the command and runs it multithreaded, 1 for
each of the files you send it.

Requires about ~12gb RAM per thread.

May take 1-2 days."""


import sys
import multiprocessing as mp
from time import strftime

import baseFuns as lan


__author__ = 'Lucas Nell'





###############################
# Arguments needed...
###############################
# ID based on naming scheme
# (e.g., 'cast_Xb9' for castaneus aligned to chrX mm9)


###############################
# EXAMPLE DICTIONARY (for 'Command' below)
###############################
# Dictionary = {
#     'input': 'castH12_X9mD.bam',  # input bam file; will change for each thread
#     'ass': '/lustre1/lan/musGenome/chr19b9.fa',  # Assembly fasta file
#     'dir': '/lustre1/lan/cast/GATK'  # Directory for bam files
# }


###############################
# Command
###############################
Command = \
'''module load java/jdk1.7.0_67\n

export file=%(input)s
export outFile=${file/rI.bam/cV.bam}
export outFile=${outFile/.bam/.g.vcf}\n

cd %(dir)s\n

java -jar /usr/local/apps/gatk/3.4.0/GenomeAnalysisTK.jar \\
-T HaplotypeCaller \\
-R %(ass)s \\
-I $file \\
--emitRefConfidence GVCF \\
-o $outFile
'''

###############################
# Read command line args
###############################

# From the ID input from bash script, get 5 main objects for further functions
Assembly, Subspp, Chr, Directory, Dict = lan.GetObjs(sys.argv[1])

# File search for bam files
bamFiles = lan.GetBAMs(Directory, Subspp, Chr + 'rI.bam')


###############################
# Running commands
# on separate threads
###############################

if __name__ == '__main__':
    print('~'*25, '\ncallVariants.py\nStarted', strftime("%D at %H:%M:%S"), '\n', '. '*3)
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



# os.system('''scp /Users/lucasnell/uga/Python/GATKpipe/callVariants.py \
# lan@xfer2.gacrc.uga.edu:~/GATKpipe''')




# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # for testing
#
# from imp import reload
# reload(lan)
# Assembly, Subspp, Suff, Directory, Dict = lan.GetObjs('cast_X9')#sys.argv[1])
#
# print(Assembly, Subspp, Suff, Directory, Dict, sep='\n')
#
# bamFiles = ['castH12_X9rI.bam', 'castH28_X9rI.bam']
# filt=[]
# c = Dict
# c['input'] = bamFiles[0]
#
# NewComm = str(Command % Dict)
# print(NewComm)
#
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~




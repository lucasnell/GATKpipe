#!/usr/local/apps/anaconda/3-2.2.0/bin/python


"""Local realignment around indels.

With some minor arguments, this compiles the command and runs it multithreaded, 1 for
each of the files you send it.

Requires ~15gb RAM per thread.

Takes from ~12 hrs to 2 days.
"""






import multiprocessing as mp
from time import strftime
import argparse as ap

import baseFuns as lan


# import sys, os, threading
# from functools import reduce
# import sys
# import re



__author__ = 'Lucas Nell'




###############################
# Arguments
###############################

# Required: bam file(s), directory where bam files are, chromosome name
# Optional: number of threads, '--doParallel' for running multithreaded

# This in command line for example *.bam aligned to chr X mm9 in parallel
# $py bam.bam foo.bam ffoo.bam -d /lustre1/lan/musDNA/cast/bam19b9 -c Xb9 -t 3
# --doParallel


###############################
# EXAMPLE DICTIONARY (for 'Command' below)
###############################
# Dictionary = {
#     'input': 'castH12_X9mD.bam',  # input bam file; will change for each thread
#     'ass': '/lustre1/lan/musGenome/fasta/chr19b9.fa',  # Assembly fasta file
#     'dir': '/lustre1/lan/musDNA/cast/bamXb9'  # Directory for bam files
# }


###############################
# Command
###############################
Command = \
'''module load java/jdk1.7.0_67
module load samtools/latest\n

export file=%(input)s
export assembly=%(ass)s\n

cd %(dir)s\n

java -jar /usr/local/apps/gatk/3.4.0/GenomeAnalysisTK.jar \\
-T RealignerTargetCreator \\
-R ${assembly} \\
-I ${file} \\
-o ${file/mD.bam/rI.list}\n

java -jar /usr/local/apps/gatk/3.4.0/GenomeAnalysisTK.jar \\
-T IndelRealigner \\
-R ${assembly} \\
-I ${file} \\
-targetIntervals ${file/mD.bam/rI.list} \\
-o ${file/mD.bam/rI.bam}\n

samtools index -b ${file/mD.bam/rI.bam}
'''

###############################
# Setting up parser
###############################

ScriptDescript = '''Local realignment around indels'''

Parser = ap.ArgumentParser(description = ScriptDescript)
Parser.add_argument('-d', '--directory',  type = str, metavar = 'D',
                    help = 'Full path to bam file(s)', required = True)
Parser.add_argument('-c', '--chromosome', type = str, metavar = 'C',
                    choices = ['X', 'Y', 'Xb9', '19', '19b9'], required = True,
                    help = 'The chromosome bam file(s) was/were aligned to')
Parser.add_argument('-t', '--threads',  type = int, metavar = 'T',
                    required = False, default = 1,
                    help = '# threads to use (default=1); ignored if not run in parallel')
Parser.add_argument('files', type = str, metavar = 'F', nargs = '+',
                    help = 'bam file(s) that have duplicates marked (end in "mD.bam"')
Parser.add_argument('--doParallel', dest='parallel', action='store_true',
                    help = 'Should it be run in parallel? The default is False.')
Parser.set_defaults(parallel = False)


# -----------------------------
# Now reading the arguments
# -----------------------------
args = vars(Parser.parse_args())




Directory = args['directory']
Chromosome = args['chromosome']
if args['parallel']:
    Threads = args['threads']
else:
    Threads = 1
bamFiles = args['files']

# Assembly file, derived from the chromosome name
Assembly = lan.getAssem(Chromosome)
# Common components for later replacing parts of 'Command'
Dict = {'ass': Assembly, 'dir': Directory}








# ====================================================================================
# ====================================================================================
# # Previous way: using only the ID
# ====================================================================================
# ====================================================================================

# # From the ID input from bash script, get 5 main objects for further functions
# Assembly, Subspp, Suff, Directory, Dict = lan.GetObjs(sys.argv[1])
#
# # File search for bam files
# bamFiles = lan.GetBAMs(Directory, Subspp, Suff + 'mD.bam')


###############################
# Running commands
# on separate threads
###############################

if __name__ == '__main__':
    print('~'*25, '\nreIndels.py\nStarted', strftime("%D at %H:%M:%S"), '\n', '. '*3)
    if Threads > 1:
        processes = []
        for b in bamFiles:
            t = mp.Process(target = lan.DoComm, args = (b, Command, Dict))
            processes.append(t)
        # Start the processes
        for p in processes:
            p.start()
        # Ensure all of the processes have finished
        for p in processes:
            p.join()
    else:
        for b in bamFiles:
            lan.DoComm(b, Command, Dict)
    print('Process Ended', strftime("%D at %H:%M:%S"), '\n' + '~'*25)



# os.system('''scp /Users/lucasnell/uga/Python/GATKpipe/reIndels.py \
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
# bamFiles = ['castH12_X9mD.bam', 'castH28_X9mD.bam']
# filt=[]
# c = Dict
# c['input'] = bamFiles[0]
#
# NewComm = str(Command % Dict)
# print(NewComm)
#
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
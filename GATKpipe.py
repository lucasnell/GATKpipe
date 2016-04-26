#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Main file for running GATK pipeline on DNA-seq data."""




import argparse as ap
import base

'''
[prepRef >] addRG > markDups > reIndels > callVariants > jointGeno > vcfToTab >  
'''


# ====================================================================================
# ====================================================================================

#  Setting up parser

# ====================================================================================
# ====================================================================================

Parser = ap.ArgumentParser(prog = 'GATKpipe',
                           description = "Python wrapper for GATK's Best Practices " + \
                                         'pipeline.')
subParsers = Parser.add_subparsers(title = 'Subcommands', 
                                   description = 'Valid subcommands to pass to GATKpipe.')

# --------------
# Prepare reference fasta
# --------------
prParser = subParsers.add_parser('prepRef', 
                                 help = 'Prepare a reference fasta for GATK pipeline.')
prParser.add_argument('-r', '--reference', metavar = 'R',
                      help = "Path to uncompressed reference fasta file.")

# --------------
# Add read groups
# --------------
rgParser = subParsers.add_parser('addRG', help = '')
rgParser.add_argument('-c', '--cores', type=int, metavar='C', default=1,
                    help="Maximum number of cores to use. Defaults to 1.")
rgParser.add_argument('files', metavar='F', nargs='+',
                    help="BAM input file(s).")

# --------------
# Mark duplicates
# --------------
mdParser = subParsers.add_parser('markDups', help = '')
mdParser.add_argument('-c', '--cores', type=int, metavar='C', default=1,
                      help="Maximum number of cores to use. Defaults to 1.")
mdParser.add_argument('files', metavar='F', nargs='+',
                      help="BAM input file(s).")

# --------------
# Realign indels
# --------------
riParser = subParsers.add_parser('reIndels', help = '')
riParser.add_argument('-r', '--reference', metavar='R',
                      help="Path to uncompressed reference fasta file. It is assumed " + \
                           "that all input BAM files are aligned to this reference.")
riParser.add_argument('-c', '--cores', type=int, metavar='C', default=1,
                      help="Maximum number of cores to use. Defaults to 1.")
riParser.add_argument('files', metavar='F', nargs='+',
                      help="BAM input file(s).")

# --------------
# Call variants
# --------------
cvParser = subParsers.add_parser('callVars', help = '')
cvParser.add_argument('-r', '--reference', metavar='R',
                      help="Path to uncompressed reference fasta file. It is assumed " + \
                           "that all input BAM files are aligned to this reference.")
cvParser.add_argument('-c', '--cores', type = int, metavar = 'C', default = 1,
                      help = "Maximum number of cores to use. Defaults to 1.")
cvParser.add_argument('files', metavar = 'F', nargs = '+',
                      help="BAM input file(s).")

# --------------
# Joint genotyping
# --------------
jgParser = subParsers.add_parser('jointGeno',
                                 help = 'Joint genotyping via GATK GenotypeGVCFs.')
jgParser.add_argument('-r', '--reference', metavar = 'R', required = True,
                      help = "Path to uncompressed reference fasta file. It is assumed " + \
                             "that all input files are aligned to this reference.")
jgParser.add_argument('-o', '--outputName', metavar = 'O', required = True,
                      help = "Name of output file, not including extensions or " + \
                             "'_jG' suffix.")
jgParser.add_argument('-c', '--cores', type = int, metavar = 'C', default = 1,
                      help = "Maximum number of cores to use. Defaults to 1.")
jgParser.add_argument('-m', '--moreOptions', metavar = 'M', default = '',
                      help = "A single string with additional options to pass " + \
                             "to GenotypeGVCFs. If it contains spaces, be sure " + \
                             "to wrap in double-quotes.")
jgParser.add_argument('files', metavar = 'F', nargs = '+',
                      help = "gVCF input file(s) that will be jointly genotyped.")

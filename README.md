`GATKpipe`
=====
Python wrapper for The Genome Analysis Toolkit's Best Practices pipeline
-----


This pipeline was developed and tested on the newest high-performance computing cluster 
("Sapelo") at the Georgia Advanced Computing Resource Center, University of Georgia.

- OS: 64-bit CentOS 6.5 
- Torque Resource Manager
- Moab Workload Manager

The only parts specific to this system might be the following in the shell scripts 
located in the `base.py` file:

- `module load foo`, where `foo` is a software package
- Paths to PICARD (`/usr/local/apps/picard/latest/picard.jar`)
- Paths to GATK (`/usr/local/apps/gatk/latest/GenomeAnalysisTK.jar`)




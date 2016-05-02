`GATKpipe`
=====
Python wrapper for The Genome Analysis Toolkit's Best Practices pipeline
-----


This pipeline was developed and tested on the newest high-performance computing cluster 
("Sapelo") at the Georgia Advanced Computing Resource Center, University of Georgia.

- OS: 64-bit CentOS 6.5 
- Torque Resource Manager
- Moab Workload Manager

Parts specific to this system that you'll want to change include the following:

* In `GATKpipe.py`:
  - The sha-bang (`#!/usr/local/apps/anaconda/3-2.2.0/bin/python`)
* In `base.py`:
  - `module load foo`, where `foo` is a software package
  - Paths to PICARD (`/usr/local/apps/picard/latest/picard.jar`)
  - Paths to GATK (`/usr/local/apps/gatk/latest/GenomeAnalysisTK.jar`)




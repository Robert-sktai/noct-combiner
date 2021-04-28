#!/bin/bash
mkdir -p /swing
mount -t nfs -o vers=3,rw,nolock,tcp,hard,intr,retry=2,retrans=5,timeo=600 a1pp-isilonf800-dmi.sktai.io:/ifs/home/swing /swing

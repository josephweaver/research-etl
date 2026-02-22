#!/bin/bash --login

{SBATCH_LINES}
#Exmaple:
#SBATCH -t 01:00:00
#SBATCH -c 2
#SBATCH --mem=8G
#SBATCH -J etl-setup-be0e4c
#SBATCH -o /mnt/scratch/weave151/work/260222/122824-be0e4c11/logs/setup/etl-setup-be0e4c11958d45cf8677dd433a85a4ef-%j.out
set -euo pipefail

{SECERT_LINES}

{DIRECTORY_LINES}
# Example:
#log_step 'creating setup directories...'
#mkdir -p /mnt/scratch/weave151/etl

{MODULE_LINES}
# example:
# log_step 'Loading modules...'
# log_step 'loading module: Python/3.11.3-GCCcore-12.3.0'
# module load Python/3.11.3-GCCcore-12.3.0
# log_step 'loading module: rclone/1.63.1-amd64'
# module load rclone/1.63.1-amd64

{SOURCE_CONTROL_LINES}
# Example:
# log_step 'check out codes'
# if [ -f "/mnt/scratch/weave151/etl/research-etl/4f3c2a1" ]
#   mkdir -p /mnt/scratch/weave151/etl/research-etl/4f3c2a1

#   log_step 'checking etl engine.'
#   git clone https://github.com/josephweaver/research-etl.git main /mnt/scratch/weave151/etl/research-etl/4f3c2a1

#   log_step 'Setting etl engine to specific historical state'
#   git checkout 4f3c2a1
# fi 

# log_step 'check out all project pipelines.'
# if [-f "/mnt/scratch/weave151/etl/landcore-etl-pipelines/weave151-prism-vpdmax_download"]
#   mkdir -p /mnt/scratch/weave151/etl/landcore-etl-pipelines/weave151-prism-vpdmax_download

#   log_step 'checking landcore-etl-pipelines.'
#   git clone https://github.com/josephweaver/landcore-etl-pipelines.git weave151-prism-vpdmax_download /mnt/scratch/weave151/etl/landcore-etl-pipelines/weave151-prism-vpdmax_download
# fi 

# if [ -f "/mnt/scratch/weave151/etl/shared-etl-pipelines/main" ]
#   mkdir -p /mnt/scratch/weave151/etl/shared-etl-pipelines/main
  
#   log_step 'checking landcore-etl-pipelines.'
#   git clone https://github.com/josephweaver/shared-etl-pipelines.git main
# fi 

{VENV_LINES}
# Exmaple:
# if [ -f "/mnt/scratch/weave151/etl/.venv"]
#   mkdir -p /mnt/scratch/weave151/etl/.venv

#   log_step 'build the virtual environmen'
#   # build the virtual environment based on /mnt/scratch/weave151/etl/research-etl/4f3c2a1/requirements.txt
# fi

{INSTALL_LINES}
# example:
#   log_step 'install etl package.'
#   pip install etl

log_step 'setup complete'
echo setup complete
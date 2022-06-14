#!/usr/bin/bash
set -e

ENVIRONMENT=python3

# Reading input parameters
instance_type="${1:-$"ml.t3.xlarge"}" # takes one of valid aws sagemaker instance types; defaults to ml.t3.xlarge
job_id="${2:-$(date +%s)}" # takes a string or int. Usually, epoch time; defaults to current epoch time

echo "job id: ${job_id}, instance type: ${instance_type}"
jobs_list=("multi_touch_attribution")

CONDA_BASE=$(conda info --base)
source ${CONDA_BASE}/etc/profile.d/conda.sh

if { conda env list | grep "py3710_data_apps"; } >/dev/null 2>&1; then
    echo "conda env py3710_data_apps already exists"
    conda activate py3710_data_apps
else
    echo "Creating conda env py3710_data_apps"
    conda create -n "py3710_data_apps" python=3.7.10 -y
    conda activate py3710_data_apps
fi

yes | pip install -r requirements.txt --quiet

for job in ${jobs_list[@]}; do
    echo "$(date): Running ${job}"
    python launch_sagemaker_job.py --job ${job} --instance ${instance_type} --id ${job_id}
done

conda deactivate

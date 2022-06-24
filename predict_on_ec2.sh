export PATH="/home/ec2-user/anaconda3/bin:$PATH"
sh full_pipeline.sh predict ml.t3.xlarge 
conda activate py3710_data_apps
yes | pip install boto --quiet 

python stop_ec2_instance.py
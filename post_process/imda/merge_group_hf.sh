epoch=$1
set -e
#python imda_hf_merge.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART1_HF "speaker, session, channel"  /data/geyu/imda_merged_$epoch/PART1_merged 100 64

#python imda_hf_merge.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART2_HF "speaker, session, channel" /data/geyu/imda_merged_$epoch/PART2_merged 100 64

#python imda_hf_merge.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART3_HF "speaker, id, script_type, mic_type, conf, interval_id"  /data/geyu/imda_merged_$epoch/PART3_merged 100 80

python imda_hf_merge.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART4_HF "speakers, room_type, interval_id" /data/geyu/imda_merged_$epoch/PART4_merged 100 80

python imda_hf_merge.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART5_HF "speakers, category, script_type, interval_id" /data/geyu/imda_merged_$epoch/PART5_merged 100 80

python imda_hf_merge.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART6_HF "speakers, category, partition, interval_id" /data/geyu/imda_merged_$epoch/PART6_merged 100 80


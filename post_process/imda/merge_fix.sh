set -e 
#python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART1_HF "speaker, session, channel"  /data/geyu/imda_merged_fixed/PART1_merged /data/geyu/cache 
#rm -rf /data/geyu/cache
#python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART2_HF "speaker, session, channel" /data/geyu/imda_merged_fixed/PART2_merged /data/geyu/cache  
#rm -rf /data/geyu/cache
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART3_HF "speaker, id, script_type, mic_type, conf, interval_id" /data/geyu/imda_merged_fixed/PART3_merged /data/geyu/cache 
rm -rf /data/geyu/cache
#python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART4_HF "speakers, room_type, interval_id" /data/geyu/imda_merged_fixed/PART4_merged /data/geyu/cache 
#rm -rf /data/geyu/cache
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART5_HF "speakers, category, script_type, interval_id"  /data/geyu/imda_merged_fixed/PART5_merged /data/geyu/cache 
rm -rf /data/geyu/cache
#python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART6_HF "speakers, category, partition, interval_id" /data/geyu/imda_merged_fixed/PART6_merged /data/geyu/cache 
#rm -rf /data/geyu/cache


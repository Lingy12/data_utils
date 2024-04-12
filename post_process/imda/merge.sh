# python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART1_HF "speaker, session, channel"  /data/geyu/imda_merged/PART1_merged /data/geyu/cache 
set -e
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART2_HF "speaker, session, channel" /data/geyu/imda_merged/PART2_merged /data/geyu/cache  
rm -rf /data/geyu/cache
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART3_HF "speaker, id, script_type, mic_type, conf, interval_id" /data/geyu/imda_merged/PART3_merged /data/geyu/cache 
rm -rf /data/geyu/cache
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART4_HF "speakers, room_type, interval_id" /data/geyu/imda_merged/PART4_merged /data/geyu/cache 
rm -rf /data/geyu/cache
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART5_HF "speakers, category, script_type, interval_id"  /data/geyu/imda_merged/PART5_merged /data/geyu/cache 
rm -rf /data/geyu/cache
python merge_speech_cache.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART6_HF "speakers, category, partition, interval_id" /data/geyu/imda_merged/PART6_merged /data/geyu/cache 
rm -rf /data/geyu/cache


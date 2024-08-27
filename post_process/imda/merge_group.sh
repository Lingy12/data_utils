epoch_tag=$1
set -e
python merge_speech_cache_group.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART1_HF "speaker, session, channel"  "speaker," /data/geyu/imda_merged_$epoch_tag/PART1_merged /data/geyu/cache 64
rm -rf /data/geyu/cache
python merge_speech_cache_group.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART2_HF "speaker, session, channel" "speaker," /data/geyu/imda_merged_$epoch_tag/PART2_merged /data/geyu/cache  64
rm -rf /data/geyu/cache
python merge_speech_cache_group.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART3_HF "speaker, id, script_type, mic_type, conf, interval_id" "speaker, id, script_type, mic_type, conf" /data/geyu/imda_merged_$epoch_tag/PART3_merged /data/geyu/cache 64
rm -rf /data/geyu/cache
python merge_speech_cache_group.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART4_HF "speakers, room_type, interval_id" "speakers, room_type" /data/geyu/imda_merged_$epoch_tag/PART4_merged /data/geyu/cache 64
rm -rf /data/geyu/cache
python merge_speech_cache_group.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART5_HF "speakers, category, script_type, interval_id"  "speakers, category, script_type" /data/geyu/imda_merged_$epoch_tag/PART5_merged /data/geyu/cache 64
rm -rf /data/geyu/cache
python merge_speech_cache_group.py /data/geyu/archive/dropbox/IMDA_HF_v1/PART6_HF "speakers, category, partition, interval_id" "speakers, category, parition" /data/geyu/imda_merged_$epoch_tag/PART6_merged /data/geyu/cache 64
rm -rf /data/geyu/cache


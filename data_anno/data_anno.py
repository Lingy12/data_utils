from numpy import add
from time import sleep
import streamlit as st
import os
import json 
from glob import glob
from pathlib import Path
import random

DS_CONF = 'ds_conf.json'
@st.cache_resource
def load_ds_conf():
    with open(DS_CONF, 'r') as f:
        ds_conf = json.load(f)
    return ds_conf

def add_ds(name, path):
    with open(DS_CONF, 'r') as f:
        ds_conf = json.load(f)
    
    ds_conf[name] = path

    with open(DS_CONF, 'w') as f:
        json.dump(ds_conf, f)
    
    st.cache_resource.clear()

def delete_ds(name):
    with open(DS_CONF, 'r') as f:
        ds_conf = json.load(f)
    
    del ds_conf[name]

    with open(DS_CONF, 'w') as f:
        json.dump(ds_conf, f)
    
    st.cache_resource.clear()

def add_annotation(target_txt, text):
    label_file = Path(target_txt).with_suffix('.label')
    with open(label_file, 'w') as f:
        f.write(text)

def select_data(unlabelled_lst):
    index = random.choice(range(len(unlabelled_lst)))
    target_txt = unlabelled_lst[index]
    target_wav = Path(target_txt).with_suffix('.wav')
    
    return target_txt, target_wav, index

st.title('Data Annotation Tools')


ds_conf = load_ds_conf()
option = st.selectbox(
        "select the dataset you want to annotate",
        tuple(ds_conf),
        index=0,
        placeholder='Select the target dataset'
        )

assert option
wav_files = glob(os.path.join(ds_conf[option]["path"], '**/*.wav'), recursive=True)
transcription_files = glob(os.path.join(ds_conf[option]['path'], '**/*.txt'), recursive=True)
labels = glob(os.path.join(os.path.join(ds_conf[option]['path'], '**/*.label')), recursive=True)

labeled_id = set(map(lambda x: Path(os.path.basename(x)).with_suffix(''), labels))

if "unlabeled_txt_files" not in st.session_state:
    st.session_state["unlabeled_txt_files"] = list(filter(lambda x: Path(os.path.basename(x)).with_suffix('') not in labeled_id, transcription_files))

if 'target_txt' not in st.session_state:
    st.session_state['target_txt'], st.session_state['target_wav'], st.session_state['index'] = select_data(st.session_state["unlabeled_txt_files"])
# target_txt = random.choice(unlabbled_txt_files)
# target_wav = Path(target_txt).with_suffix('.wav')
# st.write('{} files need to be labeled'.format(len(st.session_state["unlabeled_txt_files"])))

anno_progress = st.progress(len(transcription_files) - len(st.session_state["unlabeled_txt_files"]), text='{}/{}'.format(
    len(transcription_files) - len(st.session_state["unlabeled_txt_files"]), len(transcription_files)
    ))

audio_file = open(st.session_state['target_wav'], 'rb')
audio_bytes = audio_file.read()
st.audio(audio_bytes, format='audio/wav')

with open(st.session_state['target_txt'], 'r') as f:
    transcript = f.read()
st.write('Transcription: {}'.format(transcript))

col1, col2, col3 = st.columns(3)

with col1: 
    if st.button('correct'):
        add_annotation(st.session_state['target_txt'], 'wrong')
        # unlabbled_txt_files.pop(st.session_state['target_txt'])
        st.session_state["unlabeled_txt_files"].pop(st.session_state['index'])

with col2:
    if st.button('wrong'):
        add_annotation(st.session_state['target_txt'], 'wrong')
        # unlabbled_txt_files.pop(st.session_state['target_txt'])
        st.session_state["unlabeled_txt_files"].pop(st.session_state['index']) 
        # sleep(0.5)
with col3:
    if st.button('skip'):
        st.session_state['target_txt'], st.session_state['target_wav'], st.session_state['index'] = select_data(st.session_state['unlabeled_txt_files'])
        # sleep(0.5)
       

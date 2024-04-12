from my_dataloader import MyHFLoader


data_loader = MyHFLoader(filter_long=True)

ds = data_loader.recursive_fault_tolerent_load('/home/geyu/gigaspeech_hf/')
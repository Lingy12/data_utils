from val_utils_mp import init_vad_model, get_va_length
from glob import glob
import os
import random
from tqdm import tqdm
import multiprocessing as mp
import json
import fire

def check_folder(root):
    mp3_files = glob(os.path.join(root, '**', '*.mp3'), recursive=True)
    # if len(mp3_files) < trail:
    #     return {"ms": 0, "en": 0}
    res = []
    pid = os.getpid()  # Get process ID
    for f in tqdm(mp3_files, desc=f"PID: {pid}"):  # Display PID in progress bar description
        f_res = get_va_length(f)
        f_res['file_name'] = f
        res.append(f_res)
        # print(f_res)
    return res

def verify_folder(args):
    root, folder = args
    init_vad_model()
    res = check_folder(os.path.join(root, folder))
    return res
    
def verify_all(root, workers=4):
    sub_dirs = ['@Misstamchiak', '@QianyuSG', 
           '@jusjusrunsworld', '@JeremyFPS', '@TheDailyKetchupPodcast', '@DarenYoong', 
           '@singtel', '@swanhangoutz', '@SingaporeEye', '@JazePhua', '@mindefsg', 
           '@DefensePoliticsAsia', '@SingaporeCivilDefenceForce', '@thecommonfolkssg9263', 
           '@TheMichelleChongChannel', '@CathedraloftheGoodShepherdSG', '@tri333ple', '@tzechi', 
           '@pxdkitty', '@SGRoadVigilanteSGRV', '@Jecsphotos', '@MYBBTC', '@YouGotWatch', '@tradingwithrayner', 
           '@JTeamSingapore', '@mrbrown', '@rwsentosa', '@LADIESFIRSTTVSG', '@OGS.Official', '@WiserBiker', 
           '@joshconsultancy', '@jianhao', '@BenjaminKhengMusic', '@TheRSAF', '@SGHeavyVehicles', '@PNNKomsan', 
           '@3Wheelingtots', '@sgcarmartreviews', '@DoctorTristanPeh', '@ZULAsg', '@EatbookYT', '@jebbey', '@TheHabitsDoctor', 
           '@DrWealthVideos', '@itsclarityco', '@Butterworks', '@ricemediaco', '@nikfatimahismail', '@MotoristSg', '@MothershipSG', 
           '@ggbleed', '@HoneyMoneySG', '@ninijiang', '@easwaranspeaks3511', '@pmosingapore', '@TheIkigaiZone', '@thetitanpodcast', 
           '@CNAInsider', '@SingaporeTourismBoard', '@corporatebreakoutcouple', '@sgheadline', '@AlexTeo', '@nopengoo', 
           '@overkillsingapore', '@goodyfeedbluecats', '@asiaone', '@ridhwannabe', '@TemasekDigital', '@MuslimSg', 
           '@ICA_Singapore', '@sosolomon22', '@OfficialZX', '@TiffwithMi', '@theonlinecitizenasia', '@TinyCreatureHub', 
           '@SGRV', '@OKLETSGO', '@tambakoverlanders', '@threesixzero', '@ImasPhua', '@VisitSingapore', '@RunnerKao', 
           '@TheFifthPersonChannel', '@ProvidendSG', '@wpsgp', '@qhventures', '@TheSmartLocal', '@cikgurebekah3957', 
           '@OurSingaporeArmy', '@Sixiderchannel', '@BusyDaddyCooks', '@AJVMFISHING', '@thewokesalaryman', '@SUSUTVC', 
           '@NTUCFairPriceSG', '@ProgressSingaporePartyOfficial', '@bpjeyaytc', '@SingaporePoliceForce', '@thewinsty', 
           '@bongqiuqiu', '@DanielTamago', '@ImanFandiAhmad', '@ChurchOfTheHolyCrossSingapore', '@teamtitanshorts', '@Zeeebo', 
           '@ShawnIskandar', '@CatholicSG', '@TheTradingGeek', '@SingaporeLifeChurch', '@evaleeqxx', '@paranormalboyz179', 
           '@ieatishootipost', '@samsungsingapore', '@GovTechSG', '@DemiZhuang', '@Swizzyinsg', '@BasicModelsManagement', '@myancrypto', 
           '@zaobaodotsg', '@HomeCentralsg', '@thetengcompany', '@savvyericchiew', '@canalplusmyanmarfg', '@MandaiWildlifeReserve', '@TheBackstageBunch', '@ViuSingapore', '@WELLOshow', '@joeyyap', '@SMRTCorpSG', '@Follow_ussg', '@SupernaturalConfessions', '@JustKeepThinking', '@DearStraightPeople', '@KelvinLearnsInvesting', '@KFCSingapore', '@singaporeair', '@BenRanAway', '@MissFizaO', '@FASTVSingapore', '@kingkongmp', '@straitstimesonline', '@McDSG', '@MOMsingapore', '@AimRun', '@JosephPrince', '@saltandlightsg', '@CPFvideos', '@LTAsingapore', '@TODAYonline', '@1m65', '@chenliao585', '@speedkakireviews', '@OompaLoompaCycling', '@cartoonnetworkasia', '@Sethisfy', '@ZermattNeo', '@wahbanana', '@SmallCityIsland', '@theroycelee', '@GospelPartnerOfficial', '@spudstudy', '@AnnetteLeeMusic', '@TEAMTITANOFFICIAL', '@fcbcsg', '@TYPICALSG', '@KleoYan', '@PastorJasonLim', '@endowus', '@rebelssquad', '@smashsports2003', '@CaffeMartellaSingapore']
    params = [(root, sub_dir) for sub_dir in sub_dirs]
    
    over_all = []
    with mp.Pool(workers) as p:
        # Each result is processed separately and tqdm is used to show progress for each folder
        results = list(tqdm(p.imap_unordered(verify_folder, params), total=len(params), desc="Main Progress"))
    
    for res in results:
        over_all.extend(res)
    
    with open('vad_para.json', 'w') as f:
        json.dump(over_all, f, indent=4, ensure_ascii=False)
    
if __name__ == '__main__':
    fire.Fire(verify_all)

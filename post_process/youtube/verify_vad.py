from val_utils import get_va_length

from glob import glob
import os 
import random
from tqdm import tqdm
import multiprocessing as mp
import os
import json
import fire

def check_folder(root):
    mp3_files = glob(os.path.join(root, '**', '*.mp3'), recursive=True)
    # print(mp3_files)
    # if len(mp3_files) < trail:
    #     return {"ms": 0, "en": 0}
    # random.choices(mp3_files)
    res = []
    for f in tqdm(mp3_files):
        # score = identify_language(f)
        f_res = get_va_length(f)
        f_res['file_name'] = f
        res.append(f_res)
        print(f_res)
        
        # agg_top_10 = update_score(agg_top_10, score)
    return res

def verify_folder(args):
    root, folder = args
    res = check_folder(os.path.join(root, folder))
    return res
    
def verify_all(root, workers=4):
    # sub_dirs = os.listdir(root)
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
    # with mp.Pool(processes=workers) as p:
    #     results = tqdm(p.imap_unordered(verify_folder, params), total=len(params))
    results = []
    for p in tqdm(params):
        results.append(verify_folder(p))
    over_all = []
    for res in results:
        over_all.append(res)
    
    with open('vad.json', 'w') as f:
        json.dump(over_all, f)
    
    # print('Valid channels are: {}'.format(valid_channel))

if __name__ == '__main__':
    fire.Fire(verify_all)
            
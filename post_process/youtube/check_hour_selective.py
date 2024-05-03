import os
import subprocess
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
import fire

def get_mp3_files(directory):
    """Yield full file paths for all mp3 files in the given directory."""
    path = Path(directory)
    return list(path.rglob('*.mp3'))

def get_duration(file_path):
    """Use ffprobe to get the duration of an MP3 file in seconds."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', str(file_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return float(result.stdout.strip())
    except ValueError:
        print(f"Error processing file: {file_path}")
        return 0

def calculate_total_duration(directory, workers=4):
    """Calculate the total duration of all MP3 files in a directory using parallel processing."""
    files = get_mp3_files(directory)
    total_duration = 0
    with Pool(workers) as pool:
        for duration in tqdm(pool.imap_unordered(get_duration, files), total=len(files), desc="Processing MP3 Files"):
            total_duration += duration
    return total_duration

# def main(directory, workers=4):
#     """Calculate the total hours of MP3 files in a given directory."""
#     if not os.path.exists(directory):
#         print("The specified directory does not exist.")
#         return

#     total_duration_seconds = calculate_total_duration(directory, workers)
#     # total_hours = total_duration_seconds / 3600
#     print(f"Total duration: {total_hours:.2f} hours")
#     return total_duration_seconds
dir_lst = ['@Misstamchiak', '@QianyuSG', '@jusjusrunsworld', '@JeremyFPS', '@TheDailyKetchupPodcast', '@DarenYoong', '@singtel', '@swanhangoutz', '@SingaporeEye', '@JazePhua', '@mindefsg', '@DefensePoliticsAsia', '@SingaporeCivilDefenceForce', '@thecommonfolkssg9263', '@TheMichelleChongChannel', '@CathedraloftheGoodShepherdSG', '@tri333ple', '@tzechi', '@pxdkitty', '@SGRoadVigilanteSGRV', '@Jecsphotos', '@MYBBTC', '@YouGotWatch', '@tradingwithrayner', '@JTeamSingapore', '@mrbrown', '@rwsentosa', '@LADIESFIRSTTVSG', '@OGS.Official', '@WiserBiker', '@joshconsultancy', '@jianhao', '@BenjaminKhengMusic', '@TheRSAF', '@SGHeavyVehicles', '@PNNKomsan', '@3Wheelingtots', '@sgcarmartreviews', '@DoctorTristanPeh', '@ZULAsg', '@EatbookYT', '@jebbey', '@TheHabitsDoctor', '@DrWealthVideos', '@itsclarityco', '@Butterworks', '@ricemediaco', '@nikfatimahismail', '@MotoristSg', '@MothershipSG', '@ggbleed', '@HoneyMoneySG', '@ninijiang', '@easwaranspeaks3511', '@pmosingapore', '@TheIkigaiZone', '@thetitanpodcast', '@CNAInsider', '@SingaporeTourismBoard', '@corporatebreakoutcouple', '@sgheadline', '@AlexTeo', '@nopengoo', '@overkillsingapore', '@goodyfeedbluecats', '@asiaone', '@ridhwannabe', '@TemasekDigital', '@MuslimSg', '@ICA_Singapore', '@sosolomon22', '@OfficialZX', '@TiffwithMi', '@theonlinecitizenasia', '@TinyCreatureHub', '@SGRV', '@OKLETSGO', '@tambakoverlanders', '@threesixzero', '@ImasPhua', '@VisitSingapore', '@RunnerKao', '@TheFifthPersonChannel', '@ProvidendSG', '@wpsgp', '@qhventures', '@TheSmartLocal', '@cikgurebekah3957', '@OurSingaporeArmy', '@Sixiderchannel', '@BusyDaddyCooks', '@AJVMFISHING', '@thewokesalaryman', '@SUSUTVC', '@NTUCFairPriceSG', '@ProgressSingaporePartyOfficial', '@bpjeyaytc', '@SingaporePoliceForce', '@thewinsty', '@bongqiuqiu', '@DanielTamago', '@ImanFandiAhmad', '@ChurchOfTheHolyCrossSingapore', '@teamtitanshorts', '@Zeeebo', '@ShawnIskandar', '@CatholicSG', '@TheTradingGeek', '@SingaporeLifeChurch', '@evaleeqxx', '@paranormalboyz179', '@ieatishootipost', '@samsungsingapore', '@GovTechSG', '@DemiZhuang', '@Swizzyinsg', '@BasicModelsManagement', '@myancrypto', '@zaobaodotsg', '@HomeCentralsg', '@thetengcompany', '@savvyericchiew', '@canalplusmyanmarfg', '@MandaiWildlifeReserve', '@TheBackstageBunch', '@ViuSingapore', '@WELLOshow', '@joeyyap', '@SMRTCorpSG', '@Follow_ussg', '@SupernaturalConfessions', '@JustKeepThinking', '@DearStraightPeople', '@KelvinLearnsInvesting', '@KFCSingapore', '@singaporeair', '@BenRanAway', '@MissFizaO', '@FASTVSingapore', '@kingkongmp', '@straitstimesonline', '@McDSG', '@MOMsingapore', '@AimRun', '@JosephPrince', '@saltandlightsg', '@CPFvideos', '@LTAsingapore', '@TODAYonline', '@1m65', '@chenliao585', '@speedkakireviews', '@OompaLoompaCycling', '@cartoonnetworkasia', '@Sethisfy', '@ZermattNeo', '@wahbanana', '@SmallCityIsland', '@theroycelee', '@GospelPartnerOfficial', '@spudstudy', '@AnnetteLeeMusic', '@TEAMTITANOFFICIAL', '@fcbcsg', '@TYPICALSG', '@KleoYan', '@PastorJasonLim', '@endowus', '@rebelssquad', '@smashsports2003', '@CaffeMartellaSingapore']

root = '/home/geyu/data/youtube_local'

len_t = 0
for d in dir_lst:
    target = os.path.join(root, d)
    len_t += calculate_total_duration(target, workers=16)

total_hours = len_t / 3600
print(f"Total duration: {total_hours:.2f} hours")

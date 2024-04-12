import re
import os
import fire
# Regular expression pattern to match and replace according to the specified rules

pattern1 = r'(%\w+\b)|(\[.*?\])|(\{&.*?\})|(\$[a-z]+\b)|(\{(breath|smack|cough|sniff|mumble|laugh|noise)\})|(#\w+\b)|(\w+~)|(&\w+\b)|(\[)|(\])|(%)|(~)'
pattern2 = r'(\{.*?\})|(~)|(\()|(\))|(#)|(&)'
pattern3 = r'<*>|\*\*|<\w*/|<\w'
# pattern = r'(%\w+\b)|(\[.*?\])|(\{(?!\&).*?\})|(\$[a-z]+\b)|(\{(breath|smack|cough|sniff|mumble|laugh|noise)\})|(#\w+\b)|(\w+~)|(&\w+\b)|([)|(])|(%)'

# Function to perform the replacements
def replace_text(text):
    def replace_match(match):
        if match.group(1):  # %xyz -> xyz
            return match.group(1)[1:]
        elif match.group(2):  # [xyz abc] -> xyz abc
            return match.group(2)[1:-1]
        elif match.group(3):  # {& x y z} -> x y z
            return match.group(3)[2:-1]
        elif match.group(4):  # $xyz -> xyz
            return match.group(4)[1:]
        elif match.group(6):  # Remove {breath}, {sniff}, {mumble}, {laugh}
            return ''
        elif match.group(7):  # #xyz -> xyz
            return match.group(7)[1:]
        elif match.group(8):  # xyz~ -> xyz
            return match.group(8)[:-1]
        else:
            return ''
    
    def replace(match):
        return ''
    text = re.sub(pattern1, replace_match, text)
    text = re.sub(pattern2, replace, text)
    text = re.sub(pattern3, replace, text)
    return text

def clean_text(source_txt, dest):
    if os.path.exists(dest):
        need_del = input('File exists, enter y to delete.')
        if need_del:
            os.remove(dest)
        else:
            print('Please remove the created file')
            exit()

    with open(source_txt, 'r') as f:
        lines = f.readlines()
    
    with open(dest, 'w') as f:
        for line in lines:
            id, sentence = line.split(' ', 1)
            sentence = replace_text(sentence).strip()
            if len(sentence) > 0:
                f.write(f'{id} {sentence}\n')

# example = '%ah %ah %ah o k lah'

# print(replace_text(example))
if __name__ == "__main__":
    fire.Fire(clean_text)

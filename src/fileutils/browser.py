import os

def traverse(folder_name = 'ddl-queries', filter=None):
    # traverse root directory, and list directories as dirs and files as files
    file_paths = []
    for root, dirs, files in os.walk(folder_name):
        path = root.split(os.sep)
        # print((len(path) - 1) * '+++', os.path.basename(root))
        for file in files:
            file_path = os.path.join(root, file)
            # print(file_path)
            # print(len(path) * '---', file)
            if filter is None or filter in file_path:
                file_paths.append(file_path)
    return file_paths

def file_skip(filename):
    # or 'centre' in filename or 'entit' in filename or  or 'center' in filename
    # if 'billing' in filename or 'biomarker' in filename or 'labreport' in filename or 'biomapping' in filename:
    #     return True 
    return False
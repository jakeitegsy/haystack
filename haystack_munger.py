from haystack_utilities import os, makedir


class Munge:

    def __init__(self, filename, to_folder=None):
        self.filename = filename
        self.ticker = (os.path.splitext(os.path.split(filename)[1])[0]
                              .split('_')[0]
                              .upper())
        self.newfile = f'{to_folder}{self.ticker}.csv'
        self.munge_file(self.filename, self.newfile)

    def munge_file(self, old_file, new_file):
        new_folder = os.path.dirname(new_file)
        old_folder = os.path.dirname(old_file)

        if not os.path.exists(old_folder):
            print(f'{old_folder} does not Exist, '
                   'Please check the name and try agan')
            return

        if old_folder != new_folder and not os.path.exists(new_file):
            makedir(new_folder)
            with open(f'{old_file}') as in_file, \
                 open(f'{new_file}', 'w') as out_file:
                 print(f"Writing '{old_file}' to '{new_file}' ...")
                 [out_file.write(line.replace('None', '0')
                                     .replace('NaN', '0')
                                     .replace('2000-01-01', '1999-12-31')) 
                  for line in in_file]
                  
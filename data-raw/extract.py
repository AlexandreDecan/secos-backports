import pandas
import subprocess
import os

# Selected ecosystem names (see libraries.io)
ECOSYSTEMS = ['Cargo', 'Packagist', 'NPM', 'Rubygems']
# Version of the libraries.io dataset
LIBRARIESIO_VERSION = '1.6.0-2020-01-12'
# Location of the libraries.io dataset
PATH_TO_LIBRARIESIO = '/data/libio1.6/'

# Fields to keep for "version-[...].csv"
VERSION_FIELDS = {
    'Platform': 'platform',
    'Project Name': 'package',
    'Number': 'version',
    'Published Timestamp': 'date',
}
# Fields to keep for "dependencies-[...].csv"
DEPENDENCY_FIELDS = {
    'Platform': 'platform',
    'Project Name': 'source',
    'Version Number': 'version',
    'Dependency Name': 'target',
    'Dependency Kind': 'kind',
    'Dependency Requirements': 'constraint',
    'Dependency Platform': 'target_platform'
}
# Kind of dependencies to keep 
DEPENDENCY_KEPT_KINDS = {
    'Cargo': ['normal', 'runtime'],
    'Packagist': ['runtime'],
    'NPM': ['runtime'],
    'Rubygems': ['runtime'],
}
# Optimization purposes:
DATA_TYPES = {
    'platform': 'category',
    'kind': 'category',
    'target': 'category',
    'constraint': 'category',
    'host': 'category',
}

if __name__ == '__main__':
    for ecosystem in ECOSYSTEMS:
        if os.path.isfile('{}-releases.csv.gz'.format(ecosystem)) and os.path.isfile('{}-dependencies.csv.gz'.format(ecosystem)):
            print('Skipping {}'.format(ecosystem))
            continue
            
        print('Extracting data for {}, this could take some time...'.format(ecosystem))
        with open('temp-releases.csv', 'w') as out:
            filename = os.path.join(PATH_TO_LIBRARIESIO, 'versions-{}.csv'.format(LIBRARIESIO_VERSION))
            subprocess.call(['head', '-1', filename], stdout=out)
            subprocess.call(['grep', ',{},'.format(ecosystem), filename], stdout=out)

        with open('temp-dependencies.csv', 'w') as out:
            filename = os.path.join(PATH_TO_LIBRARIESIO, 'dependencies-{}.csv'.format(LIBRARIESIO_VERSION))
            subprocess.call(['head', '-1', filename], stdout=out)
            subprocess.call(['grep', ',{},'.format(ecosystem), filename], stdout=out)    
            
        print('Loading data in memory')
        df_releases = pandas.read_csv(
            'temp-releases.csv',
            index_col=False,
            engine='c',
            usecols=list(VERSION_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in VERSION_FIELDS.items()},
        ).rename(columns=VERSION_FIELDS).query('platform == "{}"'.format(ecosystem))

        df_deps = pandas.read_csv(
            'temp-dependencies.csv',
            index_col=False,
            engine='c',
            usecols=list(DEPENDENCY_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in DEPENDENCY_FIELDS.items()},
        ).rename(columns=DEPENDENCY_FIELDS).query('platform == "{0}" and target_platform == "{0}"'.format(ecosystem))
        print('.. {} versions and {} dependencies loaded'.format(len(df_releases), len(df_deps)))

        print('Filtering dependencies based on "kind"')
        df_deps = df_deps.query(' or '.join(['kind == "{}"'.format(kind) for kind in DEPENDENCY_KEPT_KINDS[ecosystem]]))
        print('.. {} remaining dependencies'.format(len(df_deps)))

        print('Removing unknown packages')
        packages = df_releases['package'].drop_duplicates()
        print('.. {} known packages'.format(len(packages)))
        df_deps = df_deps.merge(
            df_releases[['package', 'version']],
            how='inner',
            left_on=['source', 'version'],
            right_on=['package', 'version'],
        ).drop(columns=['package'])
        df_deps = df_deps[df_deps['target'].isin(packages)]
        print('.. {} remaining dependencies'.format(len(df_deps)))

        print('Exporting to compressed csv')
        df_releases[['package', 'version', 'date']].to_csv(
            '{}-releases.csv.gz'.format(ecosystem),
            index=False,
            compression='gzip',
        )

        df_deps[['source', 'version', 'target', 'constraint']].to_csv(
            '{}-dependencies.csv.gz'.format(ecosystem),
            index=False,
            compression='gzip',
        )

        print('Deleting temporary files')
        subprocess.call(['rm', 'temp-releases.csv'])
        subprocess.call(['rm', 'temp-dependencies.csv'])
        print()
        
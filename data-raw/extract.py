import pandas
import subprocess
import os

# Selectec ecosystem name (see libraries.io)
ECOSYSTEM = 'NPM'
# Version of the libraries.io dataset
LIBRARIESIO_VERSION = '1.4.0-2018-12-22'
# Location of the libraries.io dataset
PATH_TO_LIBRARIESIO = '/home/alexandre/librariesio/'

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
DEPENDENCY_KEPT_KINDS = ['normal', 'runtime']


if __name__ == '__main__':
    print('Extracting data for {}, this could take some time...'.format(ECOSYSTEM))
    with open('temp-releases.csv', 'w') as out:
        filename = os.path.join(PATH_TO_LIBRARIESIO, 'versions-{}.csv'.format(LIBRARIESIO_VERSION))
        subprocess.call(['head', '-1', filename], stdout=out)
        subprocess.call(['grep', ',{},'.format(ECOSYSTEM), filename], stdout=out)
    
    with open('temp-dependencies.csv', 'w') as out:
        filename = os.path.join(PATH_TO_LIBRARIESIO, 'dependencies-{}.csv'.format(LIBRARIESIO_VERSION))
        subprocess.call(['head', '-1', filename], stdout=out)
        subprocess.call(['grep', ',{},'.format(ECOSYSTEM), filename], stdout=out)    
        
    print('Loading data in memory')
    df_releases = pandas.read_csv(
        'temp-releases.csv',
        index_col=False,
        engine='c',
        usecols=list(VERSION_FIELDS.keys())
    ).rename(columns=VERSION_FIELDS).query('platform == "{}"'.format(ECOSYSTEM))

    df_deps = pandas.read_csv(
        'temp-dependencies.csv',
        index_col=False,
        engine='c',
        usecols=list(DEPENDENCY_FIELDS.keys())
    ).rename(columns=DEPENDENCY_FIELDS).query('platform == "{0}" and target_platform == "{0}"'.format(ECOSYSTEM))
    print('.. {} versions and {} dependencies loaded'.format(len(df_releases), len(df_deps)))
    
    print('Filtering dependencies based on "kind"')
    df_deps = df_deps.query(' or '.join(['kind == "{}"'.format(kind) for kind in DEPENDENCY_KEPT_KINDS]))
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
        'releases.csv.gz',
        index=False,
        compression='gzip',
    )

    df_deps[['source', 'version', 'target', 'constraint']].to_csv(
        'dependencies.csv.gz',
        index=False,
        compression='gzip',
    )
    print('Deleting temporary files')
    subprocess.call(['rm', 'temp-releases.csv'])
    subprocess.call(['rm', 'temp-dependencies.csv'])
    print()
    
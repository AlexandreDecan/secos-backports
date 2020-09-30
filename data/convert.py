"""
This script loads data for all considered ecosystems, and:

 - Convert version number to individual components (according to semver);
 - Remove prereleases and unknown version numbers;
 - Order releases by date and by version;

"""

import pandas
import tqdm
import os

ECOSYSTEMS = ['Cargo', 'Packagist', 'NPM', 'Rubygems']

if __name__ == '__main__':
    for ecosystem in ECOSYSTEMS:
        # Skip ecosystems for which we already have data
        if os.path.isfile('./{}-releases.csv.gz'.format(ecosystem)) and os.path.isfile('./{}-dependencies.csv.gz'.format(ecosystem)):
            print('Skipping {}...'.format(ecosystem))
            continue

        print('Loading releases for {}'.format(ecosystem))
        df_releases = (
            pandas.read_csv('../data-raw/{}-releases.csv.gz'.format(ecosystem))
            .assign(date=lambda d: pandas.to_datetime(d['date'], infer_datetime_format=True))
            .dropna()
        )

        if ecosystem == 'NPM':
            print('Removing spam packages from npm')
            exclude_prefixes = ('@ryancavanaugh/pkg', 'all-packages-', 'cool-', 'neat-', 'wowdude-', 'npmdoc-', 'npmtest-', 'npm-ghost-',)
            exclude_suffixes = ('-cdn',)
            exclude_ghost = r'^ghost-\d+$'

            n, m = len(df_releases), df_releases['package'].nunique()
            df_releases = (
                df_releases
                [lambda d: ~d['package'].str.startswith(exclude_prefixes)]
                [lambda d: ~d['package'].str.endswith(exclude_suffixes)]
                [lambda d: ~d['package'].str.match(exclude_ghost)]
            )
            print('... dropped {} packages and {} versions'.format(
                m - df_releases['package'].nunique(),
                n - len(df_releases),
            ))

        print('Converting versions to semver syntax')
        df_releases[['major', 'minor', 'patch', 'misc']] = df_releases['version'].str.extract(Version.RE, expand=True)
        df_releases[['major', 'minor', 'patch']] = df_releases[['major', 'minor', 'patch']].astype(float)

        # Remove non compliant versions
        n = len(df_releases)
        print('... {} releases converted'.format(n))
        df_releases = df_releases.dropna(subset=['major', 'minor', 'patch'])
        print('... dropped {} non-compliant versions'.format(n - len(df_releases)))

        # Remove prereleases and duplicates
        n = len(df_releases)
        df_releases = (
            df_releases
            [lambda d: d['misc'].isnull()]
            .sort_values(['package', 'date'])
            .drop_duplicates(['package', 'major', 'minor', 'patch'], keep='last')
            .drop(columns=['misc'])
        )
        print('... dropped {} prereleases and duplicated versions'.format(n - len(df_releases)))


        print('Ordering releases by version and date')
        data = []
        # TODO: Look if it's really faster than doing ~ .apply(rank)
        for name, group in tqdm.tqdm(df_releases.groupby('package', sort=False)):
            group = (
                group
                # Rank by version
                .sort_values(['major', 'minor', 'patch'])
                .assign(
                    rank=lambda d: d.assign(N=1).N.cumsum(),
                )
                # Rank by date
                .sort_values(['date', 'rank'])  # Use rank if versions are distributed on the same day (e.g. imports)
                .assign(rank_date=lambda d: d.assign(N=1).N.cumsum())
            )
            data.append(group)

        print('... merging results')
        df_releases = (
            pandas.concat(data)
            .sort_values(['package', 'rank'])
            [['package', 'version', 'major', 'minor', 'patch', 'rank', 'date', 'rank_date']]
        )

        print('Persisting data on disk')
        df_releases.to_csv('./{}-releases.csv.gz'.format(ecosystem), index=False, compression='gzip')

        print()

        print('Loading dependencies for {}'.format(ecosystem))
        df_dependencies = (
            pandas.read_csv('../data-raw/{}-dependencies.csv.gz'.format(ecosystem))
            .dropna()
        )

        print('Filtering dependencies based on known packages and releases')
        n = len(df_dependencies)
        df_dependencies = (
            df_dependencies
            [lambda d: d['target'].isin(df_releases['package'])]
        )
        print('... dropped {} dependencies (unknown target)'.format(n - len(df_dependencies)))

        n = len(df_dependencies)
        df_dependencies = (
            df_dependencies
            .merge(
                df_releases[['package', 'version', 'rank']],
                how='inner',
                left_on=['source', 'version'],
                right_on=['package', 'version'],
            )
            .drop(columns=['package'])
        )
        print('... dropped {} dependencies (unknown source)'.format(n - len(df_dependencies)))

        print('Persisting data on disk')
        df_dependencies.to_csv('./{}-dependencies.csv.gz'.format(ecosystem), index=False, compression='gzip')

        print()
        print()

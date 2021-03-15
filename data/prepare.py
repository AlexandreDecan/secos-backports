"""
This script loads data for all considered ecosystems, and:

 - Remove inactive packages;
 - Remove unused packages; 
 - Create a set of "required packages" and compute for each release of these packages if it is backported or not;
 - Create as et of "dependent packages", and identify the release selected by the dependency constraint. 

"""

import pandas
import numpy
import tqdm
import os

from version import Version
from parsers import parse_or_empty
from parsers import PackagistParser, NPMParser, CargoParser, RubyGemsParser

ECOSYSTEMS = ['Cargo', 'Packagist', 'NPM', 'Rubygems']
PARSERS = {
    'Cargo': CargoParser,
    'Packagist': PackagistParser,
    'NPM': NPMParser,
    'Rubygems': RubyGemsParser,
}

# Minimum number of dependents
MIN_REQUIRED = 5

# Minimal activity date
MIN_ACTIVE = pandas.to_datetime('2019-01-01')



if __name__ == '__main__':
    for ecosystem in ECOSYSTEMS:
        # Skip ecosystems for which we already have data
        if os.path.isfile('./{}-required.csv.gz'.format(ecosystem)) and os.path.isfile('./{}-dependents.csv.gz'.format(ecosystem)):
            print('Skipping {}...'.format(ecosystem))
            continue
    
        print('Loading releases and dependencies for {}'.format(ecosystem))
        df_releases = (
            pandas.read_csv('./{}-releases.csv.gz'.format(ecosystem))
            .assign(date=lambda d: pandas.to_datetime(d['date'], infer_datetime_format=True))
        )
        
        df_dependencies = (
            pandas.read_csv('./{}-dependencies.csv.gz'.format(ecosystem))
        )
        
        print('.. packages', len(df_releases.drop_duplicates('package')))
        print('.. releases:', len(df_releases))

        print('Removing inactive packages')
        packages = (
            df_releases
            .sort_values('rank_date')
            .drop_duplicates('package', keep='last')
            [lambda d: d['date'] >= MIN_ACTIVE]
            .package
        )
        df_releases = df_releases[lambda d: d['package'].isin(packages)]

        print('.. active packages:', len(df_releases.drop_duplicates('package')))
        print('.. their releases:', len(df_releases))
        
        print('Removing unused packages')
        
        df_dependents = (
            df_dependencies
            # Keep only selected packages
            [lambda d: d['source'].isin(packages) & d['target'].isin(packages)]
            # Keep only last release of each package
            .merge(
                (
                    df_releases
                    [['package', 'rank', 'rank_date']]
                    .sort_values(['package', 'rank_date'])
                    .drop_duplicates('package', keep='last')
                ),
                how='inner',
                left_on=['source', 'rank'],
                right_on=['package', 'rank'],
            )
            .drop_duplicates(['source', 'target'])  # Effectless
        )

        print('.. required packages:', len(df_dependents.drop_duplicates('target')))
        print('.. with at least', MIN_REQUIRED, 'dependents:', len(
            df_dependents
            .groupby('target', sort=False)
            .agg({'source': 'count'})
            [lambda d: d['source'] >= MIN_REQUIRED]
        ))
        
        required = (
            df_dependents
            .groupby('target', sort=False)
            .agg({'source': 'count'})
            .reset_index()
            [lambda d: d['source'] >= MIN_REQUIRED]
            .target
        )

        df_dependents = (
            df_dependents
            [lambda d: d['target'].isin(required)]
        )

        print('.. distinct dependent packages:', len(df_dependents.drop_duplicates('source')))
        print('.. dependencies:', len(df_dependents))
        
        
        print('Identifying backported updates')
        
        data = []

        for name, group in tqdm.tqdm(df_releases[lambda d: d['package'].isin(required)].groupby('package', sort=False)):
            group = (
                group
                # Identify update kind for each release
                .sort_values('rank')
                .assign(
                    kinitial=lambda d: d['major'].shift(1).isnull(),
                    kmajor=lambda d: (d['major'] - d['major'].shift(1)).clip(0, 1).astype(bool),
                    kminor=lambda d: (d['minor'] - d['minor'].shift(1)).clip(0, 1).astype(bool),
                    kpatch=lambda d: (d['patch'] - d['patch'].shift(1)).clip(0, 1).astype(bool),
                )
                .assign(kind=lambda d: d[['kinitial', 'kmajor', 'kminor', 'kpatch']].idxmax(axis=1))
                .replace({'kind': {'kinitial': 'initial', 'kmajor': 'major', 'kminor': 'minor', 'kpatch': 'patch'}})        
                .drop(columns=['kinitial', 'kmajor', 'kminor', 'kpatch'])

                # Detect highest major seen so far
                .sort_values(['date', 'rank'])
                .assign(
                    highest_major=lambda d: d['major'].expanding().max(),
                    highest_rank=lambda d: d['rank'].expanding().max(),
                )
                .assign(
                    backported=lambda d: d['highest_rank'].where(d['major'] < d['highest_major'], numpy.nan)
                )
                .drop(columns=['highest_major', 'highest_rank'])

                # A backported update could have been deployed right before it's "source release" (i.e. the one
                # being backported). We take the closest date to identify from which update a backported one
                # was created. 
                .pipe(lambda df: 
                    # Let's find the "previous" release
                    df.merge(
                        df[['date', 'rank']],
                        how='left',
                        left_on=['backported'],
                        right_on=['rank'],
                        suffixes=('', '_previous')
                    )
                    # Let's find the "next" release
                    .merge(
                        df[['date', 'rank']].assign(rank=lambda d: d['rank'] - 1),
                        how='left',
                        left_on=['backported'],
                        right_on=['rank'],
                        suffixes=('', '_next')
                    )
                    .assign(rank_next=lambda d: d['rank_next'] + 1)
                    # Put a very distant date if no next rank exists
                    .fillna({'date_next': pandas.to_datetime('1900-01-01')})
                    # Take closest date
                    .assign(backported_from=lambda d:
                        d['rank_previous'].where(abs(d['date'] - d['date_previous']) <= abs(d['date'] - d['date_next']), d['rank_next'])
                    )
                )
                # Clean unused columns
                .drop(columns=['date_previous', 'date_next', 'rank_previous', 'rank_next'])
                # Booleans for backported
                .assign(backported=lambda d: ~d['backported'].isnull())    
            )

            data.append(group)

        df_required = (
            pandas.concat(data)
            .sort_values(['package', 'rank'])
            [['package', 'version', 'major', 'minor', 'patch', 'kind', 'rank', 'date', 'rank_date', 'backported', 'backported_from']]
        )
        
        print('Saving set of required packages')
        df_required.to_csv('./{}-required.csv.gz'.format(ecosystem), index=False, compression='gzip')
        
        print('Converting constraints to intervals')
        intervals = dict()
        parser = PARSERS[ecosystem]()


        for constraint in tqdm.tqdm(df_dependents.constraint.drop_duplicates()):
            interval = parse_or_empty(parser, constraint)
            d = {'interval': interval}

            if interval.empty:
                d['empty'] = True
                d['major'] = d['minor'] = d['patch'] = d['dev'] = False
            else:
                base = interval.lower 
                d['empty'] = False
                d['major'] = Version(float('inf'), 0, 0) in interval
                d['minor'] = Version(base.major, float('inf'), 0) in interval
                d['patch'] = Version(base.major, base.minor, float('inf')) in interval
                d['dev'] = Version(1, 0, 0) > interval

            intervals[constraint] = d

        print('Identify selected releases')
        data = []

        for target, group in tqdm.tqdm(df_dependents.groupby('target', as_index=False, sort=False), leave=True, position=0):

            target_releases = (
                df_required[lambda d: d['package'] == target]
                # Convert version to usable objects
                .assign(version=lambda d: d['version'].apply(lambda v: Version(v)))
                # Sort in decreasing order so we can easily find the "highest" accepted version given a constraint
                .sort_values('rank', ascending=False)
            )

            # Let's group by constraint, so we do not evaluate a same (target, constraint) twice.
            for constraint, cgroup in group.groupby('constraint', as_index=False, sort=False):
                # Find highest version accepted by a constraint
                d = intervals[constraint]
                interval = d['interval']
                selected = numpy.nan

                for release in target_releases.itertuples():
                    if release.version in interval:
                        selected = release.rank
                        break  # Because they are sorted by descending rank

                data.append((
                    cgroup.assign(
                        interval=str(interval),
                        selected=selected,
                        c_empty=d['empty'],
                        c_dev=d['dev'],
                        c_major=d['major'],
                        c_minor=d['minor'],
                        c_patch=d['patch'],
                    )
                ))

        print('Saving set of dependent packages')
        df_dependents = (
            pandas.concat(data)
            .sort_values(['source', 'target'])
            [['source', 'version', 'rank', 'target', 'constraint', 'interval', 'selected', 'c_empty', 'c_dev', 'c_major', 'c_minor', 'c_patch']]
        )        
        df_dependents.to_csv('./{}-dependents.csv.gz'.format(ecosystem), index=False, compression='gzip')
        
        print()
        print()
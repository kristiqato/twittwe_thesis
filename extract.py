import os
import json
import pandas as pd


def concat_data(df1, df2):
    """
    :param df1
    :param df2
    :return Dataframe
    """

    df2 = df2.convert_dtypes()
    df_concat = pd.concat([df1, df2], ignore_index=True, sort=False)
    return df_concat


def get_fake_id():
    """
    This funciton return all the id inside the fakenews directory
    """
    directory_list = list()
    for root, dirs, files in os.walk(r"code/fakenewsnet_dataset/gossipcop/fake", topdown=False):
        directory_list.append(os.path.join(root))
    ids = []
    for items in directory_list:
        id = str(str(items).split('/')[3]).strip(r"""fake\gossipcop-""")
        if 'tweet' not in id:
            ids.append(id)

    return ids


def get_tweets(id):
    """
    :param id , the account ids
    :return a csv with all the data needed for all the ids
    """
    df = pd.DataFrame()
    path = fr"code/fakenewsnet_dataset/gossipcop/fake/gossipcop-{id}/tweets/"
    for root, dirs, files in os.walk(path):
        for filename in files:
            with open(path + filename, 'r') as data:
                _df = pd.json_normalize(json.loads(data.read()))

                # df_final = _df[['created_at', 'id', 'user.id', 'user.name',
                #                 'user.screen_name', 'user.location', 'user.description',
                #                 'entities.hashtags', 'user.followers_count', 'user.friends_count', 'user.verified']]
                _df['news_id'] = id
                _df['created_at'] = pd.to_datetime(_df.created_at, errors='coerce')

                df = concat_data(df, _df)

    return df


def download_tweets():
    df = pd.DataFrame()
    for id in get_fake_id():
        if len(id) > 2:
            _df = get_tweets(id)
            df = concat_data(df, _df)

    df.to_csv('tweets.csv', index=False)


def get_retweets(id):
    df = pd.DataFrame()
    path = fr"code/fakenewsnet_dataset/gossipcop/fake/gossipcop-{id}/retweets/"
    for root, dirs, files in os.walk(path):
        for filename in files:
            with open(path + filename, 'r') as data:
                _df = pd.json_normalize(json.loads(data.read())['retweets'])

                #
                # df_final = _df[['created_at', 'id','text', 'user.id', 'user.name',
                #                 'user.screen_name', 'user.location', 'user.description',
                #                 'entities.hashtags', 'user.followers_count', 'user.friends_count', 'user.verified']]
                # df_final['news_id'] = id
                # df_final['created_at'] = pd.to_datetime(df_final.created_at, errors='coerce')

                if not _df.empty:
                    _df['news_id'] = id
                    _df['created_at'] = pd.to_datetime(_df.created_at, errors='coerce')
                    df = concat_data(df, _df)

    return df


def download_retweets():
    df = pd.DataFrame()
    for id in get_fake_id():
        if len(id) > 2:
            _df = get_retweets(id)
            if not _df.empty:
                df = concat_data(df, _df)

    df.to_csv('retweets.csv', index=False)


def get_content(id):
    """
    :param id, account id
    :return the df with all the data needed from news content
    """
    with open(r"code\fakenewsnet_dataset\gossipcop\fake\gossipcop-{}\news content.json".format(id), 'r') as file:
        dt = file.read()
        data = json.loads(dt)
        df = pd.json_normalize(data)
        df['id'] = id
        #df_final = df[['id', 'text', 'title', 'images']]

        return df


def download_content():
    """
    :return a csv with all the data from news content for all the ids
    """
    df = pd.DataFrame()
    for id in get_fake_id():
        if len(id) > 2:
            _df = get_content(id)
            df = concat_data(df, _df)

    df.to_csv('news_content.csv', index=False)


if __name__ == '__main__':
    #download_content()
    download_tweets()
    download_retweets()

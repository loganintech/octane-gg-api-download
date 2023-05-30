import json
import requests
import os
import multiprocessing

HEADERS = {"User-Agent": 'octane.gg Api Downloader'}


def get(req):
    return requests.get(req, headers=HEADERS, timeout=30).json()


def load_thing(thing):
    with open(f"{thing}.json", 'r') as f:
        return json.loads(f.read())


def download_all_thing(thing):
    def do_thing_req(page):
        return get(f'https://zsr.octane.gg/{thing}?page={page}')

    total_things = []

    page = 0
    while True:
        data = do_thing_req(page)
        if len(data[thing]) == 0:
            print(f"finished processing {thing} pages")
            print(f"total {thing} pages processed:", page)
            break

        total_things.extend(data[thing])
        if page % 10 == 0:
            print(f"Processing {thing} page", page)
        page += 1

    with open(f"{thing}.json", 'w') as f:
        f.write(json.dumps(total_things, indent=4))


def download_all_match_data():
    events = load_thing("events")

    if not os.path.exists('events'):
        os.makedirs('events')

    def download_all_event_matches():
        def downloading_event_matches(event_id):
            event_data = get(f'https://zsr.octane.gg/events/{event_id}/matches')
            with open(f"events/{event_id}-matches.json", 'w') as f:
                f.write(json.dumps(event_data, indent=4))

        for event in events:
            downloading_event_matches(event['_id'])

    def download_all_event_participants():
        def downloading_event_participants(event_id):
            event_data = get(f'https://zsr.octane.gg/events/{event_id}/participants')
            with open(f"events/{event_id}-participants.json", 'w') as f:
                f.write(json.dumps(event_data, indent=4))

        for event in events:
            downloading_event_participants(event['_id'])

    download_all_event_matches()
    download_all_event_participants()


def download_all_match_games():
    matches = load_thing("matches")

    if not os.path.exists('matches'):
        os.makedirs('matches')

    def download_match_games(match_id):
        game_data = get(f'https://zsr.octane.gg/matches/{match_id}/games')
        with open(f"matches/{match_id}-games.json", 'w') as f:
            f.write(json.dumps(game_data, indent=4))

    matches_processed = 0
    for match in matches:
        download_match_games(match['_id'])
        matches_processed += 1
        if matches_processed % 10 == 0:
            print(f"Processed match {matches_processed} of {len(matches)}")


all_stats = [
    "played",
    "wins",
    "score",
    "goals",
    "assists",
    "saves",
    "shots",
    "shootingPercentage",
    "goalParticipation",
    "rating",
    "bpm",
    "bcpm",
    "amountCollected",
    "amountCollectedBig",
    "amountCollectedSmall",
    "amountStolen",
    "amountStolenBig",
    "amountStolenSmall",
    "avgSpeed",
    "avgSpeedPercentage",
    "totalDistance",
    "countPowerslide",
    "timePowerslide",
    "avgPowerslideDuration",
    "avgDistanceToBall",
    "avgDistanceToBallPossession",
    "avgDistanceToBallNoPossession",
    "avgDistanceToMates",
    "timeSupersonicSpeed",
    "timeBoostSpeed",
    "timeSlowSpeed",
    "percentSupersonicSpeed",
    "percentBoostSpeed",
    "percentSlowSpeed",
    "timeGround",
    "timeLowAir",
    "timeHighAir",
    "percentGround",
    "percentLowAir",
    "percentHighAir",
    "countCollectedBig",
    "countCollectedSmall",
    "countStolenBig",
    "countStolenSmall",
    "amountOverfill",
    "amountOverfillStolen",
    "amountUsedWhileSupersonic",
    "timeZeroBoost",
    "timeBoost0To25",
    "timeBoost25To50",
    "timeBoost50To75",
    "timeBoost75To100",
    "timeFullBoost",
    "percentZeroBoost",
    "percentBoost0To25",
    "percentBoost25To50",
    "percentBoost50To75",
    "percentBoost75To100",
    "percentFullBoost",
    "timeDefensiveThird",
    "timeNeutralThird",
    "timeOffensiveThird",
    "timeDefensiveHalf",
    "timeOffensiveHalf",
    "timeMostBack",
    "timeMostForward",
    "percentDefensiveThird",
    "percentNeutralThird",
    "percentOffensiveThird",
    "percentDefensiveHalf",
    "percentOffensiveHalf",
    "percentMostBack",
    "percentMostForward",
    "timeBehindBall",
    "timeInfrontBall",
    "timeClosestToBall",
    "timeFarthestFromBall",
    "percentBehindBall",
    "percentInfrontBall",
    "percentClosestToBall",
    "percentFarthestFromBall",
    "inflicted",
    "taken",
]

game_record_stats = [
    "score",
    "goals",
    "assists",
    "saves",
    "shots",
    "shootingPercentage",
    "goalParticipation",
    "rating",
]


def determine_valid_record_stats():
    def do_player_stat_record_req(match_id, player_id, stat):
        url = f'https://zsr.octane.gg/records/players?type=game&player={player_id}&match={match_id}&stat={stat}'
        print(url)
        thing_req = get(url)
        return thing_req

    found_valid_stats = []

    for stat in game_record_stats:
        do_player_stat_record_req('6043145f91504896348eae05', '5f3d8fdd95f40596eae23f4d', stat)

    for stat in all_stats:
        print(f"Trying Stat: {stat}")

        data = do_player_stat_record_req('6043145f91504896348eae05', '5f3d8fdd95f40596eae23f4d', stat)
        if 'error' in data:
            print(f"[{stat}] Error: {data['error']}")
            continue

        if 'records' not in data:
            print(f"[{stat}] Didn't have an error for stat, but didn't have records key: {data}")
            continue

        records = data['records']
        if len(records) == 0:
            continue

        for record in records:
            if record['stat'] > 0:
                print(f"Found valid stat: {stat}")
                found_valid_stats.append(stat)
                break

    return found_valid_stats


def download_app_player_aggregate_stats():
    players = load_thing("players")

    def do_player_stat_req(player_id, extra=''):
        url = f'https://zsr.octane.gg/stats/players{extra}?player={player_id}&stat={"&stat=".join(all_stats)}'
        thing_req = get(url)
        return thing_req

    count = 0
    for player in players:

        folder_path = f"./stats/{player['_id']}"

        aggregate = do_player_stat_req(player['_id'])
        by_team = do_player_stat_req(player['_id'], extra="/teams")
        by_opponent = do_player_stat_req(player['_id'], extra="/opponents")
        by_event = do_player_stat_req(player['_id'], extra="/events")

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        with open(f"{folder_path}/aggregate.json", 'w') as f:
            f.write(json.dumps(aggregate, indent=4))

        with open(f"{folder_path}/by_team.json", 'w') as f:
            f.write(json.dumps(by_team, indent=4))

        with open(f"{folder_path}/by_opponent.json", 'w') as f:
            f.write(json.dumps(by_opponent, indent=4))

        with open(f"{folder_path}/by_event.json", 'w') as f:
            f.write(json.dumps(by_event, indent=4))

        count += 1
        if count % 10 == 0:
            print(f"Processed {count} players of {len(players)}")

def do_team_stat_req(team_id, extra=''):
    url = f'https://zsr.octane.gg/stats/teams{extra}?team={team_id}&stat={"&stat=".join(all_stats)}'
    thing_req = get(url)
    return thing_req

def do_team_fetch(team_id):
    folder_path = f"./team_stats/{team_id}"

    aggregate = do_team_stat_req(team_id)
    by_opponent = do_team_stat_req(team_id, extra="/opponents")
    by_event = do_team_stat_req(team_id, extra="/events")

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    with open(f"{folder_path}/aggregate.json", 'w') as f:
        f.write(json.dumps(aggregate, indent=4))
    with open(f"{folder_path}/by_opponent.json", 'w') as f:
        f.write(json.dumps(by_opponent, indent=4))
    with open(f"{folder_path}/by_event.json", 'w') as f:
        f.write(json.dumps(by_event, indent=4))


def download_team_stats():
    def download_team_aggregate_stats(batch_size=10):
        teams = load_thing("teams")

        offset = 0
        while True:
            batch = teams[offset:offset + batch_size]
            if len(batch) == 0:
                break

            procs = [multiprocessing.Process(target=do_team_fetch, args=(team['_id'],)) for team in batch]
            for proc in procs:
                proc.start()

            for proc in procs:
                proc.join()

            offset += batch_size
            print(f"Processed {offset} teams of {len(teams)}")

    download_team_aggregate_stats()


if __name__ == '__main__':
    try:
        input("This script will use 25GB RAM at it's peak, and download about 12.3 GB of data. Press anything to "
              "continue. Ctrl+C to cancel.")
    except KeyboardInterrupt:
        exit()

    if not os.path.exists("./data"):
        os.makedirs("./data")
    os.chdir("./data")

    # download_all_thing('events')
    # download_all_thing('matches')
    # download_all_thing('games')
    # download_all_thing('players')
    # download_all_thing('teams')
    # download_app_player_aggregate_stats()
    download_team_stats()

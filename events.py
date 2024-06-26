import urllib.request
import json
import os
from datetime import datetime, timezone, timedelta
import pandas as pd

now_time = datetime.now(timezone.utc) + timedelta(hours=-3) 
end_time = now_time + timedelta(hours=12) 
now = now_time.strftime('%Y-%m-%dT%H:%M:%SZ')
end = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

events = ["Tennis", "Cricket", "Australian Rules"]


class BetfairAPI:
    def __init__(self, app_key, session_token):
        self.url = "https://api.betfair.com/exchange/betting/json-rpc/v1"
        self.headers = {
            'X-Application': app_key,
            'X-Authentication': session_token,
            'content-type': 'application/json'
        }
        self.market_ids = []
        self.market_df = pd.DataFrame()

    def call_api(self, jsonrpc_req):
        req = urllib.request.Request(self.url, jsonrpc_req.encode(), self.headers)
        response = urllib.request.urlopen(req)
        jsonResponse = response.read().decode()
        return jsonResponse

    def get_event_types(self):
        event_type_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEventTypes", "params": {"filter":{ }}, "id": 1}'
        eventTypesResponse = self.call_api(event_type_req)
        eventTypeLoads = json.loads(eventTypesResponse)
        eventTypeResults = eventTypeLoads['result']
        return eventTypeResults

    def get_selection_id(self, marketCatalogueResult):
        if marketCatalogueResult is not None:
            for market in marketCatalogueResult:
                return market['runners'][0]['selectionId']
            
    def getMarketCatalogue(self, listOfMarkets):
        market_catalogue_req = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    #Select the whole sport, tennis, cricket, footy..
                    #repr removes the string
                    "marketIds": listOfMarkets,
                    #Only care about head to heads
                    "marketTypeCodes": ["MATCH_ODDS"]
                },
                "maxResults": "1000",
                "marketProjection": ["EVENT", "COMPETITION", "MARKET_START_TIME", "EVENT_TYPE", "RUNNER_DESCRIPTION"]
            }
        }
        market_catalogue_response = self.call_api(json.dumps(market_catalogue_req))
        market_catalogue_loads = json.loads(market_catalogue_response)
        #market_catalogue_results = market_catalogue_loads['result']

        return market_catalogue_loads


    def get_market_book_best_offers(self, marketId):
        market_book_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketBook", "params": {"marketIds":["' + marketId + '"],"priceProjection":{"priceData":["EX_BEST_OFFERS"]}}, "id": 1}'
        market_book_response = self.call_api(market_book_req)
        market_book_loads = json.loads(market_book_response)
        market_book_result = market_book_loads.get('result')
        if market_book_result is None:
            print('Exception from API-NG' + str(market_book_loads['error']))
            exit()
        return market_book_result

    def print_price_info(self, market_book_result):
        if market_book_result is not None:
            print('Please find Best three available prices for the runners')
            for marketBook in market_book_result:
                runners = marketBook['runners']
                for runner in runners:
                    print('Selection id is ' + str(runner['selectionId']))
                    if runner['status'] == 'ACTIVE':
                        print('Available to back price :' + str(runner['ex']['availableToBack']))
                        print('Available to lay price :' + str(runner['ex']['availableToLay']))
                    else:
                        print('This runner is not active')


def transform_timestamp(timestamp_str, *args):
        dt_object = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        return dt_object.strftime('%d/%m/%y %H:%M')

def instantiate_session():
    with open(".env", "r") as file:
        for line in file:
            if line.startswith("SESSION="):
                session = line.split("=", 1)[1].replace('"', "").strip()
                break
    os.environ["SESSION"] = session
    return BetfairAPI(app_key=os.getenv("APPLICATION_KEY"), session_token=session)

def get_event_markets(portal, event_type):
    event_filter = [item['eventType'] for item in event_type if item["eventType"]["name"] in events]

    for event in event_filter:
        event_type_id = event["id"]
        market_catalogue = portal.getMarketCatalogue(event_type_id)
        
def transform_event_dataframe (portal):
    next_markets = portal.market_df[["marketId", "marketStartTime", "totalMatched", "competition.name", "event.name", "eventType.name"]].sort_values(by="marketStartTime")
    next_markets["marketStartTime"] = next_markets["marketStartTime"].apply(transform_timestamp)
    next_markets[["home", "away"]] = next_markets["event.name"].str.split(" v ", expand=True)
    next_markets = next_markets.drop(columns=["event.name"])
    return next_markets


if __name__ == "__main__":
    portal = instantiate_session()
    event_types = portal.get_event_types()
    get_event_markets(portal, event_types)
    next_markets = transform_event_dataframe(portal)

    pricing_dictionary = {}

    markets = []
    for market in next_markets["marketId"]:
        backs = []
        backs_sizes = []
        lays = []
        lays_sizes = []
        market_info = portal.get_market_book_best_offers(marketId=market)
        markets.append(market_info)
        for item in market_info:
            market_id = item['marketId']
            total_matched = item['totalMatched']
            runners = item['runners']
            if float(total_matched) > 10000.0:
                runner = runners[0]
                selection_id = runner['selectionId']
                status = runner['status']
                available_to_lay = runner['ex']['availableToLay']

                for item in available_to_lay:
                    lays.append(float(item['price']))
                    lays_sizes.append(float(item['size']))

                available_to_back = runner['ex']['availableToBack']

                for item in available_to_back:
                    backs.append(float(item['price']))
                    backs_sizes.append(float(item['size']))
                pricing_dictionary[market_id] = {'backs': backs, 'backs_sizes': backs_sizes, 'lays': lays, 'lays_sizes': lays_sizes}

            
    for key in pricing_dictionary:
        for field in ['backs', 'backs_sizes', 'lays', 'lays_sizes']:
            pricing_dictionary[key][field] = pricing_dictionary[key].get(field, []) + [None] * (3 - len(pricing_dictionary[key].get(field, [])))

    print(pricing_dictionary)
    # Convert pricing_dictionary to a list of DataFrames
    dfs = [pd.DataFrame(pricing_dictionary[key]).assign(marketId=key) for key in pricing_dictionary]

    # Concatenate the list of DataFrames vertically
    available_home_selection = pd.concat(dfs, ignore_index=True)

    # Inner join on the "marketId" column
    merged_df = pd.merge(available_home_selection, next_markets, on='marketId', how='inner')
    print(merged_df.columns)


    #print(next_markets)


from flask import (Flask, request, jsonify)
import json
import werkzeug
from neo4j import GraphDatabase

def create_app():
    app = Flask(__name__)

    URI = "neo4j://localhost"

    with GraphDatabase.driver(URI) as driver:
        driver.verify_connectivity()
        print("Connection established.")
    
    @app.route("/cities", methods=["PUT"])
    def register_city():
        reqBody = request.json
        
        if not reqBody.get("name") or not reqBody.get("country"):
            return { "message": "Could not register the city, it exists or mandatory attributes are missing." }, 400
        
        city = driver.execute_query("""
            MERGE (c:City {name: $name, country:$country})
            return c
            """,
            name = reqBody.get("name"),
            country = reqBody.get("country"),
            database_="neo4j"
        )

        if city.records:
            return { "message": "City registered successfully." }, 204
    
        else:
            return { "message": "Could not register the city, it exists or mandatory attributes are missing." }, 400
        
    @app.route("/cities", methods=["GET"])
    def get_cities():
        country_filter = request.args.get('country')

        print(country_filter)

        if country_filter:
            cities = driver.execute_query("""
                MATCH (c:City {country: $country})
                RETURN c
                """,
                country = country_filter,
                database_ = "neo4j"
            )

        else:
            cities = driver.execute_query("""
                MATCH (c:City)
                RETURN c
                """,
                database_="neo4j"
            )
        
        cities = [{"name": city["c"]["name"], "country": city["c"]["country"]} for city in cities.records]

        return cities, 200
    
    @app.route("/cities/<name>", methods=["GET"])
    def get_city(name):
        cities = driver.execute_query("""
            MATCH (c:City {name:$name})
            RETURN c
            """,
            name = name,
            database_="neo4j"
        )

        if cities.records:
            city = [{"name": city["c"]["name"], "country": city["c"]["country"]} for city in cities.records][0]

            return city, 200
        
        else:
            return { "message": "City not found." }, 404
        
    @app.route("/cities/<name>/airports", methods=["PUT"])
    def register_airport(name):
        reqBody = request.json

        if not reqBody.get("code") or not reqBody.get("name") or not reqBody.get("numberOfTerminals") or not reqBody.get("address"):
            return { "message": "Could not register the city, it exists or mandatory attributes are missing." }, 400
        
        airport = driver.execute_query("""
            MATCH (c:City {name: $cityName})
            MERGE (a:Airport {code: $code, name:$airportName, numberOfTerminals: $numberOfTerminals, address: $address})
            MERGE (c)-[r:HAS_AIRPORT]->(a)
            """,
            cityName = name,
            code = reqBody.get("code"),
            airportName = reqBody.get("name"),
            numberOfTerminals = reqBody.get("numberOfTerminals"),
            address = reqBody.get("address"),
            database_="neo4j"
        )

        if airport:
            return { "message": "Airport created." }, 204
    
        else:
            return { "message": "Could not register the city, it exists or mandatory attributes are missing." }, 400
        
    @app.route("/cities/<name>/airports", methods=["GET"])
    def get_city_airports(name):
        airports = driver.execute_query("""
            MATCH (c:City {name: $cityName})-[:HAS_AIRPORT]->(a:Airport)
            RETURN a
            """,
            cityName = name,
            database_="neo4j"
        )

        airports = [{"code": airport["a"]["code"], "city": name, "name": airport["a"]["name"], "numberOfTerminals": airport["a"]["numberOfTerminals"], "address": airport["a"]["address"]} for airport in airports.records]

        return airports, 200
        
    @app.route("/airports/<code>", methods=["GET"])
    def get_airport(code):
        airports = driver.execute_query("""
            MATCH (c:City)-[:HAS_AIRPORT]->(a:Airport {code:$code})                         
            RETURN a, c
            """,
            code = code,
            database_="neo4j"
        )

        if airports.records:
            airport = [{"code": airport["a"]["code"], "city": airport["c"]["name"], "name": airport["a"]["name"], "numberOfTerminals": airport["a"]["numberOfTerminals"], "address": airport["a"]["address"]} for airport in airports.records][0]

            return airport, 200


        else:
            return { "message": "Airport not found." }, 404
        
    @app.route("/flights", methods=["PUT"])
    def register_flight():
        reqBody = request.json

        if not reqBody.get("number") or not reqBody.get("fromAirport") or not reqBody.get("toAirport") or not reqBody.get("price") or not reqBody.get("flightTimeInMinutes") or not reqBody.get("operator"):
            return { "message": "Could not register the city, it exists or mandatory attributes are missing." }, 400
        
        flight = driver.execute_query("""
            MATCH (from:Airport {code: $fromAirport}), (to:Airport {code: $toAirport})
            MERGE (from)-[r:FLIES_TO {number: $number, fromAirport:$fromAirport, toAirport: $toAirport, price: $price, flightTimeInMinutes: $flightTimeInMinutes, operator: $operator}]->(to)
            """,
            number = reqBody.get("number"),
            fromAirport = reqBody.get("fromAirport"),
            toAirport = reqBody.get("toAirport"),
            price = reqBody.get("price"),
            flightTimeInMinutes = reqBody.get("flightTimeInMinutes"),
            operator = reqBody.get("operator"),
            database_="neo4j"
        )

        if flight:
            return { "message": "Flight created." }, 204
    
        else:
            return { "message": "Flight could not be created due to missing data." }, 400
        
    @app.route("/flights/<code>", methods=["GET"])
    def get_flight(code):
        flights = driver.execute_query("""
            MATCH (from:Airport)-[flight:FLIES_TO {number:$code}]->(to:Airport)
            MATCH (fromCity:City)-[:HAS_AIRPORT]->(from)
            MATCH (toCity:City)-[:HAS_AIRPORT]->(to)
            RETURN from, to, flight, fromCity, toCity
            """, 
            code = code,
            database_="neo4j"
        )

        if flights.records:
            flight = [{"number": code, "fromAirport": flight["from"]["code"], "fromCity": flight["fromCity"]["name"], "toAirport": flight["to"]["code"], "toCity": flight["toCity"]["name"], "price": flight["flight"]["price"], "flightTimeInMinutes": flight["flight"]["flightTimeInMinutes"], "operator": flight["flight"]["operator"]} for flight in flights.records][0]

            return flight, 200


        else:
            return { "message": "Flight not found." }, 404
        
    @app.route("/search/flights/<fromCity>/<toCity>", methods=["GET"])
    def find_flights(fromCity, toCity):
        flights = driver.execute_query("""
            MATCH (fromCity:City {name: $fromCityName})-[:HAS_AIRPORT]->(from:Airport)
            MATCH (toCity:City {name: $toCityName})-[:HAS_AIRPORT]->(to:Airport)
            MATCH path = (from)-[flight:FLIES_TO*1..3]->(to)
            RETURN fromCity, from, toCity, to, flight, path
            """, 
            fromCityName = fromCity,
            toCityName = toCity,
            database_="neo4j"
        )

        if flights.records:
            flights = [{
                "fromAirport": flight["from"]["code"],
                "toAirport": flight["to"]["code"],
                "flights": [relationship["number"] for relationship in flight["path"].relationships],
                "price": sum([relationship["price"] for relationship in flight["path"].relationships]),
                "flightTimeInMinutes": sum([relationship["flightTimeInMinutes"] for relationship in flight["path"].relationships])
                } for flight in flights.records]

            return flights, 200

        else:
            return { "message": "Flights not found." }, 404

    @app.route("/cleanup", methods=["POST"])
    def clean_database():
        driver.execute_query("""
            MATCH (n)
            DETACH DELETE n;
            """,
            database_="neo4j"
        )

        return { "message": "Cleanup successful." }, 200
    
    return app
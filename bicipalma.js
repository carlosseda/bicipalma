const neo4j = require('neo4j-driver')
const fs = require('fs')

class BiciPalma {
  constructor() {
    this.driver = neo4j.driver(
      'bolt://localhost:7687/routes',
      neo4j.auth.basic('neo4j', 'password')
    )
    this.session = this.driver.session({ database: 'bicipalma' })
  }

  async fetchStations() {
    const response = await fetch('https://maps.nextbike.net/maps/nextbike-official.json?city=789')
    return await response.json()
  }

  async updateStationMaintenance(tx, place) {
    let result = await tx.run(`MATCH (station:Station {id: $id}) RETURN station`, { id: place.uid })
    let station = result.records[0]

    if (!station) return

    const stationProps = station.get('station').properties

    if (stationProps.maintenance !== place.maintenance) {
      
      if (place.maintenance) {
        await tx.run(
          `MERGE (station:Station {id: $id})
           SET station.maintenanceHistory = station.maintenanceHistory + {startTime: timestamp()}`,
          { id: place.uid }
        )
      } else {
        const endTime = BigInt(Date.now())
        const startTime = BigInt(stationProps.maintenanceHistory.slice(-1)[0].startTime)
        const duration = endTime - startTime

        await tx.run(
          `MATCH (station:Station {id: $id})
           SET station.maintenanceHistory[-1].endTime = $endTime,
               station.maintenanceHistory[-1].duration = $duration`,
          {
            id: place.uid,
            endTime: endTime.toString(),
            duration: duration
          }
        )
      }
    }
  }

  async updateStationAvailability(tx, place, property, historyProperty, threshold) {
    let result = await tx.run(`MATCH (station:Station {id: $id}) RETURN station`, { id: place.uid })
    let station = result.records[0]

    if (!station) return

    const stationProps = station.get('station').properties
    const currentValue = stationProps[property]
    const newValue = place[property]

    if (currentValue > newValue && newValue <= threshold) {
      await tx.run(
        `MERGE (station:Station {id: $id})
         SET station.${historyProperty} = station.${historyProperty} + {startTime: timestamp()}`,
        { id: place.uid }
      )
    } else if (currentValue <= threshold && newValue > threshold) {
      const endTime = BigInt(Date.now())
      const startTime = BigInt(stationProps[historyProperty].slice(-1)[0].startTime)
      const duration = endTime - startTime

      await tx.run(
        `MATCH (station:Station {id: $id})
         SET station.${historyProperty}[-1].endTime = $endTime,
             station.${historyProperty}[-1].duration = $duration`,
        {
          id: place.uid,
          endTime: endTime.toString(),
          duration: duration
        }
      )
    }
  }

  calculateCost(duration) {
    if (duration <= 30n * 60n * 1000n) {
      return 0
    } else if (duration <= 2n * 60n * 60n * 1000n) {
      return 0.5 * Math.ceil(Number(duration) / (30 * 60 * 1000))
    } else {
      return 1.5 + (Number(duration) - 2 * 60 * 60 * 1000) / (30 * 60 * 1000) * 3
    }
  }

  async updateBike(tx, place, bike) {
    let result = await tx.run(`MATCH (bike:Bike {id: $id}) RETURN bike`, { id: bike.number })
    let lastStateBike = result.records[0]

    if (lastStateBike) {
      const lastStateProps = lastStateBike.get('bike').properties

      if (lastStateProps.currentStation !== place.uid) {
        const endTime = BigInt(Date.now())
        const startTime = BigInt(lastStateProps.lastUpdate)
        const duration = endTime - startTime
        const cost = this.calculateCost(duration)

        const query = bike.bike_type === 150 
          ? `MATCH (start:Station), (end:Station), (bike:Bike)
             WHERE start.id = $startId AND end.id = $endId AND bike.id = $bikeNumber
             CREATE (travel:Travel {id: $travelId})
             SET travel.startTime = $startTime,
                 travel.endTime = $endTime,
                 travel.duration = $duration,
                 travel.cost = $cost
             CREATE (travel)-[:STARTED_AT]->(start)
             CREATE (travel)-[:ENDED_AT]->(end)
             CREATE (travel)-[:TRAVELLED_BY]->(bike)
             CREATE (bike)-[:TRAVELLED_ON]->(travel)`
          : `MATCH (start:Station), (end:Station), (bike:Bike)
             WHERE start.id = $startId AND end.id = $endId AND bike.id = $bikeNumber
             CREATE (travel:Travel {id: $travelId})
             SET travel.startTime = $startTime,
                 travel.endTime = $endTime,
                 travel.duration = $duration,
                 travel.cost = $cost,
                 travel.batteryConsumed = $batteryConsumed
             CREATE (travel)-[:STARTED_AT]->(start)
             CREATE (travel)-[:ENDED_AT]->(end)
             CREATE (travel)-[:TRAVELLED_BY]->(bike)
             CREATE (bike)-[:TRAVELLED_ON]->(travel)`;

        const params = {
          startId: lastStateProps.currentStation,
          endId: place.uid,
          bikeNumber: bike.number,
          travelId: `${bike.number}-${startTime}`,
          startTime: startTime.toString(),
          endTime: endTime.toString(),
          duration: parseInt(duration),
          cost: cost,
          ...(bike.bike_type === 143 ? { batteryConsumed: lastStateProps.battery - bike.pedelec_battery } : {})
        }

        await tx.run(query, params)
      }

      if (lastStateProps.active !== bike.active) {
        if (!bike.active) {
          await tx.run(
            `MERGE (bike:Bike {id: $id})
             SET bike.stateHistory = bike.stateHistory + {
                 active: $active,
                 startTime: timestamp(),
                 state: $state,
                 station: $station
             }`,
            {
              id: bike.number,
              active: bike.active,
              state: bike.state,
              station: place.uid,
            }
          )
        } else {
          const endTime = BigInt(Date.now())
          const startTime = BigInt(lastStateProps.stateHistory.slice(-1)[0].startTime)
          const duration = endTime - startTime

          await tx.run(
            `MERGE (bike:Bike {id: $id})
             SET bike.stateHistory[-1].endTime = $endTime,
                 bike.stateHistory[-1].duration = $duration`,
            {
              id: bike.number,
              endTime: endTime.toString(),
              duration: duration.toString()
            }
          )
        }
      }
    }

    await tx.run(
      `MERGE (bike:Bike {id: $id})
       SET bike.bikeType = $bikeType,
           bike.battery = $battery,
           bike.active = $active,
           bike.state = $state,
           bike.currentStation = $currentStation,
           bike.lastUpdate = timestamp()
       RETURN bike`,
      {
        id: bike.number,
        bikeType: bike.bike_type,
        battery: bike.pedelec_battery,
        active: bike.active,
        state: bike.state,
        currentStation: place.uid,
      }
    )
  }

  async updateStationsAndBikes() {
    const stations = await this.fetchStations()
    const tx = this.session.beginTransaction()

    try {
      for (let place of stations.countries[0].cities[0].places) {
        await this.updateStationMaintenance(tx, place)
        await this.updateStationAvailability(tx, place, 'bikesAvailableToRent', 'notAvailableToRentHistory', 0)
        await this.updateStationAvailability(tx, place, 'freeRacks', 'notAvailableRacksHistory', 0)

        await tx.run(
          `MERGE (station:Station {id: $id})
            SET station.name = $name,
              station.y = $y,
              station.x = $x,
              station.bikeRacks = $bikeRacks,
              station.bikes = $bikes,
              station.bikesAvailableToRent = $bikesAvailableToRent,
              station.freeRacks = $freeRacks,
              station.maintenance = $maintenance
            RETURN station`,
          {
            id: place.uid,
            name: place.name,
            y: place.lat,
            x: place.lng,
            bikeRacks: place.bike_racks,
            bikes: place.bikes,
            bikesAvailableToRent: place.bikes_available_to_rent,
            freeRacks: place.free_racks,
            maintenance: place.maintenance
          }
        )

        for (let bike of place.bike_list) {
          await this.updateBike(tx, place, bike)
        }
      }

      await tx.commit()
      console.log(`Done at ${new Date().toString()}`)
    } catch (error) {
      await tx.rollback()
      console.error(`Transaction failed: ${error}`)
      fs.writeFileSync('error.log', `[${new Date().toString()}] ${error.message}`)
    }
  }

  start() {
    this.updateStationsAndBikes()
    setInterval(() => this.updateStationsAndBikes(), 60000)
  }
}

try {
  const biciPalma = new BiciPalma()
  biciPalma.start()
} catch (error) {
  fs.writeFileSync('error.log', `[${new Date().toString()}] ${error.message}`)
}

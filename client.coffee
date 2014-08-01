util = require('util');
# Yaml = require('js-yaml');
Gearman = require('gearman').Gearman
assert = require('assert')

argv = process.argv.slice(2)

args = { hostname: argv[0] } 

if argv[1]
	args.community = argv[1]

console.log( util.inspect(args) )

mdb = null

MongoClient = require('mongodb').MongoClient
MongoClient.connect("mongodb://localhost:27017/exampleDb", (err, db) -> 
  if ! err 
    console.log "We are connected" 
    #collection = db.createCollection('snmp', (err, collection) -> 
    #    console.log 'connected to ' + collection.collectionName
    #    mdb = db
    #)
    collection = db.createCollection('snmp_agent', (err, collection) -> 
        console.log 'connected to ' + collection.collectionName
        mdb = db
    )

)

client = new Gearman('worker0',4730) 

# handle finished jobs
client.on 'WORK_COMPLETE', (job) ->
	# console.log 'job completed, result:', job.payload.toString()
	obj = JSON.parse job.payload.toString()
	# obj = Yaml.safeLoad job.payload.toString() 
	# console.log( util.inspect(obj) ) 
	# console.log 'connected to ' + collection.collectionName
	console.log(  '(' + fact.type + ' ' + ('(' + k + ':"' + v + '")' for k,v of fact.slots ).join(' ') + ')'  ) for fact in obj
	facts = []
	for fact in obj 
		console.log(  '(' + fact.type + ' ' + ('(' + k + ':"' + v + '")' for k,v of fact.slots ).join(' ') + ')'  )
		if not facts[fact.type]? 
			facts[fact.type] = []	
		facts[fact.type].push fact.slots
		#mdb.collection(fact.type).insert( [ { slots: fact.slots, metadata: { date: new Date() } } ], {w:1}, (err, result) -> 
		#	#db.snmp_agent.ensureIndex( { "metadata.date": 1  } , { expireAfterSeconds: 10 } )
		#	assert.equal(null, err); 
		#	client.close()
		#)
	for k,v of facts
		for fact in v
			mdb.createCollection( k , (err, collection) ->
				assert.equal(null, err);
				console.log 'connected to ' + collection.collectionName	
				collection.insert( [ { slots: fact, metadata: { date: new Date() } } ], {w:1}, (err, result) ->
					console.log('insert record')
					assert.equal(null, err);	
				)
			)
			


# connect to the gearman server
client.connect ->
    # submit a job to uppercase a string with normal priority in the foreground
    client.submitJob 'snmp_gather', JSON.stringify args

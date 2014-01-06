package me.ryanhamshire.GriefPrevention;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import me.ryanhamshire.GriefPrevention.Debugger.DebugLevel;
import me.ryanhamshire.GriefPrevention.exceptions.WorldNotFoundException;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.configuration.ConfigurationSection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class MongoDataStore extends DataStore {
	private DB db;
	private DBCollection claimdata;
	private DBCollection nextclaimid;
	private DBCollection playerdata;

	private String password;
	private String userName;

	public MongoDataStore(ConfigurationSection Source,
			ConfigurationSection Target) throws Exception {
		initialize(Source, Target);
	}

	@Override
	synchronized void close() {
		super.close();
	}

	// deletes a top level claim from the database
	@Override
	synchronized void deleteClaimFromSecondaryStorage(Claim claim) {
		// while we get returns from removals, keep removing
		while (claimdata.findAndRemove(new BasicDBObject("id", claim.id)) != null)
			;
		while (claimdata.findAndRemove(new BasicDBObject("parentid", claim.id)) != null)
			;
	}

	@Override
	public boolean deletePlayerData(String playerName) {
		// if playerdata was removed
		return playerdata.findAndRemove(new BasicDBObject("name", playerName)) != null;
	}

	@Override
	public List<PlayerData> getAllPlayerData() {
		super.ForceLoadAllClaims(this);
		List<PlayerData> generateList = new ArrayList<PlayerData>();
		try {
			for (DBObject doc : playerdata.find()) {
				// name,lastlogin,accruedblocks,bonusblocks
				String pname = (String) doc.get("name");
				Date lastlog = (Date) doc.get("lastlogin");
				int accrued = (Integer) doc.get("accruedblocks");
				int bonus = (Integer) doc.get("bonusblocks");
				PlayerData pd = new PlayerData();
				pd.playerName = pname;
				pd.lastLogin = lastlog;
				pd.accruedClaimBlocks = accrued;
				pd.bonusClaimBlocks = bonus;
				generateList.add(pd);
			}
			return generateList;
		} catch (Exception exx) {

		}
		return new ArrayList<PlayerData>();
	}

	@Override
	synchronized PlayerData getPlayerDataFromStorage(String playerName) {
		PlayerData playerData = new PlayerData();
		playerData.playerName = playerName;

		DBObject doc = playerdata
				.findOne(new BasicDBObject("name", playerName));
		// if there's no data for this player, create it with defaults
		if (doc == null) {
			this.savePlayerData(playerName, playerData);
		}
		//
		// otherwise, just read from the database
		else {
			playerData.lastLogin = (Date) doc.get("lastlogin");
			playerData.accruedClaimBlocks = (Integer) doc.get("accruedblocks");
			playerData.bonusClaimBlocks = (Integer) doc.get("bonusblocks");
			playerData.ClearInventoryOnJoin = (Boolean) doc.get("clearonjoin");
		}

		return playerData;
	}

	@Override
	public boolean hasPlayerData(String pName) {
		return null != playerdata.findOne(new BasicDBObject("name", pName));
	}

	@Override
	synchronized void incrementNextClaimID() {
		this.setNextClaimID(this.nextClaimID + 1);
	}

	@Override
	void initialize(ConfigurationSection Source, ConfigurationSection Target)
			throws Exception {

		String grabhost = Source.getString("Host", "localhost");
		String grabport = Source.getString("Port", "27017");
		String grabdbname = Source.getString("Database", "GriefPrevention");
		db = null;

		userName = Source.getString("Username", "");
		password = Source.getString("Password", "");
		try {
			MongoClient mongoClient = new MongoClient(grabhost + ':' + grabport);
			db = mongoClient.getDB(grabdbname);
		} catch (UnknownHostException e) {
			// invalid host
		} finally {
			if (db == null) {
				GriefPrevention.AddLogEntry("Could not connect to MongoDB at "
						+ grabhost + ':' + grabport);
				return;
			} else {
				GriefPrevention
						.AddLogEntry("Successfully connected to MongoDB");
				if (!userName.isEmpty()) {
					try {
						db.authenticate(userName, password.toCharArray());
					} catch (MongoException e) {
						// invalid login
					}
				}
			}
		}
		
		claimdata = db.getCollection("claimdata");
		playerdata = db.getCollection("playerdata");
		nextclaimid = db.getCollection("nextclaimid");
		
		Target.set("Username", userName);
		Target.set("Password", password);

		// load group data into memory
		if(playerdata.findOne() != null)
		for (DBObject doc : playerdata.find()) {
			if(!doc.containsField("name"))
				continue;
			String name = (String) doc.get("name");

			// ignore non-groups. all group names start with a dollar sign.
			if (!name.startsWith("$"))
				continue;

			String groupName = name.substring(1);
			if (groupName == null || groupName.isEmpty())
				continue; // defensive coding, avoid unlikely cases

			int groupBonusBlocks = (Integer) doc.get("bonusblocks");

			this.permissionToBonusBlocksMap.put(groupName, groupBonusBlocks);
		}

		DBObject nextId = nextclaimid.findOne(null, null, new BasicDBObject(
				"nextid", -1));
		if (nextId != null) {
			this.nextClaimID = (Long) nextId.get("nextid");
		}
		// or create new nextClaimId
		else {
			nextclaimid.insert(new BasicDBObject("nextid", (long) 0));
			this.nextClaimID = (long) 0;
		}

		super.initialize(Source, Target);
	}

	// updates the database with a group's bonus blocks
	@Override
	synchronized void saveGroupBonusBlocks(String groupName, int currentValue) {
		// group bonus blocks are stored in the player data table, with player
		// name = $groupName
		String playerName = "$" + groupName;
		PlayerData playerData = new PlayerData();
		playerData.bonusClaimBlocks = currentValue;

		this.savePlayerData(playerName, playerData);
	}

	// saves changes to player data. MUST be called after you're done making
	// changes, otherwise a reload will lose them
	@Override
	synchronized public void savePlayerData(String playerName,
			PlayerData playerData) {
		// never save data for the "administrative" account. an empty string for
		// player name indicates administrative account
		if (playerName.length() == 0)
			return;
		try {
			playerdata.findAndModify(
					new BasicDBObject("name", playerName), null, null, false,
					new BasicDBObject("lastlogin", playerData.lastLogin)
							.append("accruedblocks",
									playerData.accruedClaimBlocks)
							.append("bonusblocks", playerData.bonusClaimBlocks)
							.append("clearonjoin",
									playerData.ClearInventoryOnJoin), false, true);
		} catch (MongoException e) {
			GriefPrevention.AddLogEntry("Unable to save data for player "
					+ playerName + ".  Details:");
			e.printStackTrace();
		}
	}

	public synchronized long getNextClaimID() {
		return this.nextClaimID;
	}

	// sets the next claim ID. used by incrementNextClaimID() above, and also
	// while migrating data from a flat file data store
	@Override
	public synchronized void setNextClaimID(long nextID) {
		this.nextClaimID = nextID;

		try {
			DBObject updateMe = nextclaimid.findOne();
			nextclaimid.remove(updateMe);
			nextclaimid.insert(new BasicDBObject("nextid", nextID));
		} catch (MongoException e) {
			GriefPrevention.AddLogEntry("Unable to set next claim ID to "
					+ nextID + ".  Details:");
			GriefPrevention.AddLogEntry(e.getMessage());
		}
	}

	@Override
	void WorldLoaded(World loading) {
		try {
			Debugger.Write(
					"Database:Loading claims in world:" + loading.getName(),
					DebugLevel.Verbose);

			ArrayList<Claim> claimsToRemove = new ArrayList<Claim>();
	
			for (DBObject doc : claimdata.find(new BasicDBObject("world",
					loading.getName()))) {
				try {
					// skip subdivisions

					long parentId = (Long) doc.get("parentid");
					if (parentId != -1)
						continue;

					long claimID = (Long) doc.get("id");

					String lesserCornerString = (String) doc
							.get("lessercorner");

					String greaterCornerString = (String) doc
							.get("greatercorner");

					String ownerName = (String) doc.get("owner");

					String buildersString = (String) doc.get("builders");
					String[] builderNames = buildersString.split(";");

					String containersString = (String) doc.get("containers");
					String[] containerNames = containersString.split(";");

					String accessorsString = (String) doc.get("accessors");
					String[] accessorNames = accessorsString.split(";");

					String managersString = (String) doc.get("managers");
					String[] managerNames = managersString.split(";");

					boolean neverdelete = (Boolean) doc.get("neverdelete");

					Location lesserBoundaryCorner = this
							.locationFromString(lesserCornerString);
					Location greaterBoundaryCorner = this
							.locationFromString(greaterCornerString);

					Claim topLevelClaim = new Claim(lesserBoundaryCorner,
							greaterBoundaryCorner, ownerName, builderNames,
							containerNames, accessorNames, managerNames,
							claimID, neverdelete);

					// search for another claim overlapping this one
					Claim conflictClaim = this.getClaimAt(
							topLevelClaim.lesserBoundaryCorner, true);

					// if there is such a claim, mark it for later removal
					if (conflictClaim != null) {
						claimsToRemove.add(conflictClaim);
						continue;
					}

					// otherwise, add this claim to the claims collection
					else {
						this.claims.add(topLevelClaim);
						topLevelClaim.inDataStore = true;
					}

					// look for any subdivisions for this claim
					for (DBObject childDoc : claimdata.find(new BasicDBObject(
							"parentid", topLevelClaim.id))) {
						lesserCornerString = (String) childDoc
								.get("lessercorner");
						lesserBoundaryCorner = this
								.locationFromString(lesserCornerString);
						Long subid = (Long) childDoc.get("id");
						greaterCornerString = (String) childDoc
								.get("greatercorner");
						greaterBoundaryCorner = this
								.locationFromString(greaterCornerString);

						buildersString = (String) childDoc.get("builders");
						builderNames = buildersString.split(";");

						containersString = (String) childDoc.get("containers");
						containerNames = containersString.split(";");

						accessorsString = (String) childDoc.get("accessors");
						accessorNames = accessorsString.split(";");

						managersString = (String) childDoc.get("managers");
						managerNames = managersString.split(";");

						neverdelete = (Boolean) doc.get("neverdelete");

						Claim childClaim = new Claim(lesserBoundaryCorner,
								greaterBoundaryCorner, ownerName, builderNames,
								containerNames, accessorNames, managerNames,
								null, neverdelete);

						// add this claim to the list of children of the current
						// top level claim
						childClaim.parent = topLevelClaim;
						topLevelClaim.children.add(childClaim);
						childClaim.subClaimid = subid;
						childClaim.inDataStore = true;
					}
				} catch (MongoException e) {
					GriefPrevention
							.AddLogEntry("Unable to load a claim.  Details: "
									+ e.getMessage() + " ... " + doc.toString());
					e.printStackTrace();
				} catch (WorldNotFoundException e) {
					// We don't need to worry about this exception.
					// This is just here to catch it so that the plugin
					// can load without erroring out.
				}
			}

			for (int i = 0; i < claimsToRemove.size(); i++) {
				this.deleteClaimFromSecondaryStorage(claimsToRemove.get(i));
			}

		} catch (Exception exx) {
			System.out
					.println("Exception from databaseDataStore handling of WorldLoad-");
			exx.printStackTrace();
		}

	}

	// actually writes claim data to the database
	synchronized private void writeClaimData(Claim claim) {
		String lesserCornerString = this.locationToString(claim
				.getLesserBoundaryCorner());
		String greaterCornerString = this.locationToString(claim
				.getGreaterBoundaryCorner());
		String owner = claim.claimOwnerName; // we need the direct name, so
												// Admin Claims aren't lost.

		ArrayList<String> builders = new ArrayList<String>();
		ArrayList<String> containers = new ArrayList<String>();
		ArrayList<String> accessors = new ArrayList<String>();
		ArrayList<String> managers = new ArrayList<String>();

		claim.getPermissions(builders, containers, accessors, managers);

		String buildersString = "";
		for (int i = 0; i < builders.size(); i++) {
			buildersString += builders.get(i) + ";";
		}

		String containersString = "";
		for (int i = 0; i < containers.size(); i++) {
			containersString += containers.get(i) + ";";
		}

		String accessorsString = "";
		for (int i = 0; i < accessors.size(); i++) {
			accessorsString += accessors.get(i) + ";";
		}

		String managersString = "";
		for (int i = 0; i < managers.size(); i++) {
			managersString += managers.get(i) + ";";
		}

		long parentId;
		long id;
		if (claim.parent == null) {
			parentId = -1;
		} else {
			parentId = claim.parent.id;

			id = claim.getSubClaimID() != null ? claim.getSubClaimID()
					: claim.parent.children.indexOf(claim);
		}

		if (claim.id == null) {
			id = claim.getSubClaimID() != null ? claim.getSubClaimID() : -1;
		} else {
			id = claim.id;
		}

		try {
			claimdata
					.findAndModify(
							new BasicDBObject("id", id),
							null,
							null,
							false,
							new BasicDBObject("owner", owner)
									.append("lessercorner", lesserCornerString)
									.append("greatercorner",
											greaterCornerString)
									.append("builders", buildersString)
									.append("containers", containersString)
									.append("accessors", accessorsString)
									.append("managers", managersString)
									.append("parentid", parentId)
									.append("neverdelete",
											(claim.neverdelete ? 1 : 0)),
							false, true);
			Debugger.Write("updated data into griefprevention_claimdata- ID:"
					+ claim.getID(), DebugLevel.Verbose);
		} catch (MongoException e) {

			GriefPrevention.AddLogEntry("Unable to save data for claim at "
					+ this.locationToString(claim.lesserBoundaryCorner)
					+ ".  Details:");
			e.printStackTrace();
		}
	}

	@Override
	synchronized void writeClaimToStorage(Claim claim) // see datastore.java.
														// this
														// will ALWAYS be a top
														// level claim
	{
		try {
			// wipe out any existing data about this claim
			// this.deleteClaimFromSecondaryStorage(claim);

			// write top level claim data to the database
			this.writeClaimData(claim);

			// for each subdivision
			for (int i = 0; i < claim.children.size(); i++) {
				// write the subdivision's data to the database
				this.writeClaimData(claim.children.get(i));
			}
		} catch (MongoException e) {
			GriefPrevention.AddLogEntry("Unable to save data for claim at "
					+ this.locationToString(claim.lesserBoundaryCorner)
					+ ".  Details:");
			e.printStackTrace();
		}
	}
}

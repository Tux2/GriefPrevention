package me.ryanhamshire.GriefPrevention;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class YamlDataStore extends DataStore {
    // YAML Paths
    // Use constants to prevent typo bugs
    private final static String CLAIM_FILENAME = dataLayerFolderPath + File.separator + "claims.yml";
    private final static String PLAYER_FILENAME = dataLayerFolderPath + File.separator + "players.yml";
    private final static String SERVER_FILENAME = dataLayerFolderPath + File.separator + "server.yml";
    private final static String DATE_FORMAT = "yyyy.MM.dd.HH.mm.ss";

    // Player constants
    private final static String LASTLOGIN_PATH = "LastLogin";
    private final static String ACCRUEDBLOCKS_PATH = "AccruedBlocks";
    private final static String BONUSBLOCKS_PATH = "BonusBlocks";
    private final static String CLEARINVENTORY_PATH = "ClearInventoryOnJoin";

    // Claim constants
    private final static String LESSERBOUNDARY_PATH = "LesserBoundary";
    private final static String GREATERBOUNDARY_PATH = "GreaterBoundary";
    private final static String OWNER_PATH = "Owner";
    private final static String BUILDERS_PATH = "Builders";
    private final static String CONTAINER_PATH = "Containers";
    private final static String ACCESSOR_PATH = "Accessors";
    private final static String MANAGER_PATH = "Managers";
    private final static String CHILDREN_PATH = "Children";
    private final static String PARENT_PATH = "Parent";
    private final static String NEVERDELETE_PATH = "NeverDelete";
    private final static String MODIFIED_PATH = "LastModified";

    // Server constants
    private final static String NEXTCLAIMID_PATH = "NextClaimId";
    private final static String GROUPBONUS_PATH = "GroupBonusBlocks";

    private YamlConfiguration claimConfig;
    private YamlConfiguration playerConfig;
    private YamlConfiguration serverConfig;

    /**
     * Creates an instace of YamlDataStore
     */
    YamlDataStore() throws Exception {
        this(getSourceCfg(), getTargetCfg());
    }

    // initialization!
    public YamlDataStore(FileConfiguration Source, FileConfiguration Target) throws Exception {
        this.initialize(Source, Target);
    }

    private static FileConfiguration getSourceCfg() {
        File f = new File(DataStore.dataLayerFolderPath + "yaml.yml");
        if (f.exists())
            return YamlConfiguration.loadConfiguration(f);
        return new YamlConfiguration();
    }

    private static FileConfiguration getTargetCfg() {
        return new YamlConfiguration();
    }

    /**
     * Reloads all data fom the YAML file
     */
    public void reloadAll() {
        reloadClaims();
        reloadPlayers();
        reloadServerData();
    }

    /**
     * Reloads claim data from the YAML file
     */
    public void reloadClaims() {
        claimConfig =  YamlConfiguration.loadConfiguration(new File(CLAIM_FILENAME));
    }

    /**
     * Reloads player data from the YAML file
     */
    public void reloadPlayers() {
        playerConfig = YamlConfiguration.loadConfiguration(new File(PLAYER_FILENAME));
    }

    /**
     * Reloads server data from the YAML file
     */
    public void reloadServerData() {
        serverConfig = YamlConfiguration.loadConfiguration(new File(SERVER_FILENAME));

        // Initialize Claim ID if needed
        if (!serverConfig.isSet(NEXTCLAIMID_PATH)) {
            serverConfig.set(NEXTCLAIMID_PATH, 0);
        }
    }

    /**
     * Writes all data to the YAML file
     */
    public void saveAll() {
        saveClaims();
        savePlayers();
        saveServerData();
    }

    /**
     * Writes claim data to the YAML file
     */
    public void saveClaims() {
        try {
            claimConfig.save(CLAIM_FILENAME);
        } catch (IOException ex) {
            Debugger.Write("Failed to save " + CLAIM_FILENAME + ".", Debugger.DebugLevel.Errors);
        }
    }

    /**
     * Writes player data to the YAML file
     */
    public void savePlayers() {
        try {
            playerConfig.save(PLAYER_FILENAME);
        } catch (IOException ex) {
            Debugger.Write("Failed to save " + PLAYER_FILENAME + ".", Debugger.DebugLevel.Errors);
        }
    }

    /**
     * Writes server data to the YAML file
     */
    public void saveServerData() {
        try {
            serverConfig.save(SERVER_FILENAME);
        } catch (IOException ex) {
            Debugger.Write("Failed to save " + SERVER_FILENAME + ".", Debugger.DebugLevel.Errors);
        }
    }

    @Override
    void initialize(ConfigurationSection Source, ConfigurationSection Target) throws Exception {
        super.initialize(Source, Target);
        reloadAll();
        // load group data into memory
        if (serverConfig.isConfigurationSection(GROUPBONUS_PATH)) {
            ConfigurationSection groupReader = serverConfig.getConfigurationSection(GROUPBONUS_PATH);

            for(String s : groupReader.getKeys(false)) {
                this.permissionToBonusBlocksMap.put(s.replace("-", "."), groupReader.getInt(s));
            }
        }

        // load next claim number into memory
        this.nextClaimID = serverConfig.getLong(NEXTCLAIMID_PATH);
    }

    @Override
    public void close() {
        super.close();
        saveAll();
    }

    @Override
    public boolean deletePlayerData(String playerName) {
        playerConfig.set(playerName.toLowerCase(), null);
        return false;
    }

    @Override
    public List<PlayerData> getAllPlayerData() {
        List<PlayerData> pData = new ArrayList<>();

        for (String player : playerConfig.getKeys(false)) {
            pData.add(this.getPlayerData(player));
        }
        return pData;
    }

    @Override
    PlayerData getPlayerDataFromStorage(String playerName) {
        playerName = playerName.toLowerCase();
        PlayerData pData = new PlayerData();

        // Create a new player data object if it doesn't exist
        if (!playerConfig.isConfigurationSection(playerName)) {
            this.savePlayerData(playerName, pData);
            return pData;
        }

        // Otherwise read the file
        ConfigurationSection playerReader = playerConfig.getConfigurationSection(playerName);

        // Read the login timestamp
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        if (playerReader.isString(LASTLOGIN_PATH)) {
            try {
                pData.lastLogin = dateFormat.parse(playerReader.getString(LASTLOGIN_PATH));
            } catch (ParseException ex) {
                GriefPrevention.AddLogEntry("Unable to load last login for \"" + playerName + "\".");
                //It's broke, reset it back to default
                Calendar lastYear = Calendar.getInstance();
                lastYear.add(Calendar.YEAR, -1);
                pData.lastLogin = lastYear.getTime();
            }
        }

        // Read the accrued claim blocks
        if (playerReader.isInt(ACCRUEDBLOCKS_PATH)) {
            pData.accruedClaimBlocks = playerReader.getInt(ACCRUEDBLOCKS_PATH);
        }

        // Read the bonus claim blocks granted by administrators
        if (playerReader.isInt(BONUSBLOCKS_PATH)) {
            pData.bonusClaimBlocks = playerReader.getInt(BONUSBLOCKS_PATH);
        }

        // Read if the inventory should be cleared
        if (playerReader.isBoolean(CLEARINVENTORY_PATH)) {
            pData.ClearInventoryOnJoin = playerReader.getBoolean(CLEARINVENTORY_PATH);
        }

        return pData;
    }

    @Override
    public void savePlayerData(String playerName, PlayerData playerData) {
        // never save data for the "administrative" account. an empty string for
        // claim owner indicates administrative account    }
        if (playerName == null || playerName.isEmpty()) {
            return;
        }
        playerName = playerName.toLowerCase();

        // Create the configuration section if it does not exist
        ConfigurationSection playerWriter;
        if (!playerConfig.isConfigurationSection(playerName)) {
            playerWriter = playerConfig.createSection(playerName);
        } else {
            playerWriter = playerConfig.getConfigurationSection(playerName);
        }

        // Write the last login timestamp
        if (playerData.lastLogin == null) {
            playerData.lastLogin = new Date();
        }
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        playerWriter.set(LASTLOGIN_PATH, dateFormat.format(playerData.lastLogin));

        // Write the accured claim blocks
        playerWriter.set(ACCRUEDBLOCKS_PATH, playerData.accruedClaimBlocks);

        // Write the bonus claim blocks
        playerWriter.set(BONUSBLOCKS_PATH, playerData.bonusClaimBlocks);

        // Write the inventory clear bit
        playerWriter.set(CLEARINVENTORY_PATH, playerData.ClearInventoryOnJoin);
    }

    @Override
    public boolean hasPlayerData(String playerName) {
        return playerConfig.isConfigurationSection(playerName.toLowerCase());
    }

    @Override
    void WorldLoaded(World worldLoad) {
        for(String s : claimConfig.getKeys(false)) {
            ConfigurationSection claimReader = claimConfig.getConfigurationSection(s);
            if(claimReader.getString(GREATERBOUNDARY_PATH).split(";")[0].equals(worldLoad.getName()) && !claimReader.isString(PARENT_PATH)) {
               Claim readClaim = getClaimFromStorage(Long.valueOf(s));
               if(readClaim != null) {
                   readClaim.inDataStore = true;
                   addClaim(readClaim);
               }
            }
        }
    }

    Claim getClaimFromStorage(Long id) {
        if(!claimConfig.isConfigurationSection(String.valueOf(id))) {
            return null;
        }

        ConfigurationSection claimReader = claimConfig.getConfigurationSection(String.valueOf(id));
        Claim topLevelClaim;
        try {
            Location lesserBoundaryCorner = this.locationFromString(claimReader.getString(LESSERBOUNDARY_PATH));
            Location greaterBoundaryCorner = this.locationFromString(claimReader.getString(GREATERBOUNDARY_PATH));
            String ownerName = claimReader.getString(OWNER_PATH);
            boolean neverDelete = claimReader.getBoolean(NEVERDELETE_PATH);

            List<String> builders = (claimReader.getStringList(BUILDERS_PATH));
            List<String> containers = (claimReader.getStringList(CONTAINER_PATH));
            List<String> accessors = (claimReader.getStringList(ACCESSOR_PATH));
            List<String> managers = (claimReader.getStringList(MANAGER_PATH));

            topLevelClaim = new Claim(lesserBoundaryCorner, greaterBoundaryCorner, ownerName,
                    builders.toArray(new String[builders.size()]), containers.toArray(new String[containers.size()]),
                    accessors.toArray(new String[accessors.size()]), managers.toArray(new String[managers.size()]),
                    id, neverDelete);

            topLevelClaim.modifiedDate = new Date(claimReader.getLong(MODIFIED_PATH));

            if(claimReader.isList(CHILDREN_PATH)) {
                for (long subclaimId : claimReader.getLongList(CHILDREN_PATH)) {
                    Claim child = getClaimFromStorage(subclaimId); // Yay recursion!
                    if(child != null) {
                        child.parent = topLevelClaim;
                        topLevelClaim.children.add(child);
                        child.inDataStore = true;
                    }
                }
            }


        } catch (Exception ex) {
            GriefPrevention.AddLogEntry("Unable to load data for claim \"" + id + "\": " + ex.getClass().getName() + "-" + ex.getMessage());
            ex.printStackTrace();
            return null;
        }
        return topLevelClaim;
    }

    @Override
    void writeClaimToStorage(Claim claim) {
        if(claim.id < 0) claim.id = getNextClaimID();
        String claimID = String.valueOf(claim.id);

        ConfigurationSection claimWriter;
        if(claimConfig.isConfigurationSection(claimID)) {
            claimWriter = claimConfig.getConfigurationSection(claimID);
        } else {
            claimWriter = claimConfig.createSection(claimID);
        }

        // Write claim information
        claimWriter.set(LESSERBOUNDARY_PATH, this.locationToString(claim.getLesserBoundaryCorner()));
        claimWriter.set(GREATERBOUNDARY_PATH, this.locationToString(claim.getGreaterBoundaryCorner()));
        claimWriter.set(OWNER_PATH, claim.getOwnerName());
        claimWriter.set(NEVERDELETE_PATH, claim.neverdelete);
        if(claim.parent != null) {
            claimWriter.set(PARENT_PATH, claim.parent.getID());
        }

        // Write the trusted players
        // There is a cleaner way to do this, but it would mean adding getters in Claim
        // Maybe a future enahancement later
        List<String> builders = new ArrayList<>();
        List<String> containers = new ArrayList<>();
        List<String> accessors = new ArrayList<>();
        List<String> managers = new ArrayList<>();

        claim.getPermissions(builders, containers, accessors, managers);

        claimWriter.set(BUILDERS_PATH, builders);
        claimWriter.set(CONTAINER_PATH, containers);
        claimWriter.set(ACCESSOR_PATH, accessors);
        claimWriter.set(MANAGER_PATH, managers);
        claimWriter.set(MODIFIED_PATH, new Date().getTime());


        // Write children information

        // It's ok to do this for any claim type,
        // If it's a subdivision, it will be an empty list
        // Recursion for the win!
        List<Long> children = new ArrayList<>();
        for(Claim child : claim.children) {
            children.add(child.getID());
            writeClaimToStorage(child);
        }
        if(!children.isEmpty()) {
            claimWriter.set(CHILDREN_PATH, children);
        }
    }

    // Deletes a claim from the datastore
    @Override
    void deleteClaimFromSecondaryStorage(Claim claim) {
        claimConfig.set(claim.getID().toString(), null);
    }

    @Override
    public long getNextClaimID() {
        return this.nextClaimID;
    }

    @Override
    void incrementNextClaimID() {
        setNextClaimID(++this.nextClaimID);
    }

    @Override
    public void setNextClaimID(long nextClaimId) {
        serverConfig.set(NEXTCLAIMID_PATH, nextClaimId);
    }

    @Override
    void saveGroupBonusBlocks(String groupName, int amount) {
        ConfigurationSection groupWriter;
        if (serverConfig.isConfigurationSection(GROUPBONUS_PATH)) {
            groupWriter = serverConfig.getConfigurationSection(GROUPBONUS_PATH);
        } else {
            groupWriter = serverConfig.createSection(GROUPBONUS_PATH);
        }

        // Periods are key delimiters in YAML, need to use something else
        groupWriter.set(groupName.replace('.', '-'), amount);
    }

    @Override
    public int getGroupBonusBlocks(String playerName) {
        int totalBonusBlocks = 0;
        if (!serverConfig.isConfigurationSection(GROUPBONUS_PATH)) {
            return totalBonusBlocks;
        }


        ConfigurationSection groupReader = serverConfig.getConfigurationSection(GROUPBONUS_PATH);
        Player player = GriefPrevention.instance.getServer().getPlayer(playerName);
        if (player == null) {
            return totalBonusBlocks;
        }

        for(String s : groupReader.getKeys(false)) {
            // Periods are key delimiters in YAML, need to use something else
            if(player.hasPermission(s.replace('-', '.'))) {
                totalBonusBlocks += serverConfig.getInt(s);
            }
        }

        return totalBonusBlocks;
    }
}

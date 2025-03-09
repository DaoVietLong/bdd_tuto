from manager import (initialize_bloom_filters, register_player, get_player_profile)

def main():
    # Step 1: Initialize Bloom filters in Redis
    print("Initializing Bloom filters...")
    initialize_bloom_filters()
    print("Bloom filters initialized.\n")
    
    # Step 2: Register players with unique usernames and in-game names
    print("Registering new players...")
    
    # Register a player with unique credentials
    username = "player1"
    ingame = "gamer1"
    email = "player1@example.com"
    password = "password123"
    result = register_player(username, ingame, email, password)
    print(f"Registering {username} with ingame name {ingame}: {result}")
    
    # Try to register a player with the same username (should fail)
    duplicate_username = "player1"
    unique_ingame = "gamer2"
    email2 = "player2@example.com"
    password2 = "password456"
    result = register_player(duplicate_username, unique_ingame, email2, password2)
    print(f"Registering {duplicate_username} with a new ingame name {unique_ingame}: {result}")
    
    # Try to register a player with the same in-game name (should fail)
    unique_username = "player3"
    duplicate_ingame = "gamer1"
    email3 = "player3@example.com"
    password3 = "password789"
    result = register_player(unique_username, duplicate_ingame, email3, password3)
    print(f"Registering {unique_username} with an existing ingame name {duplicate_ingame}: {result}")
    
    # Register another player with unique username and in-game name
    username4 = "player4"
    ingame4 = "gamer4"
    email4 = "player4@example.com"
    password4 = "password101"
    result = register_player(username4, ingame4, email4, password4)
    print(f"Registering {username4} with ingame name {ingame4}: {result}")
    
    print("\nPlayer registration testing completed.\n")

    # Step 3: Retrieve a player profile from MongoDB
    print("Retrieving player profiles...")
    
    # Attempt to retrieve a profile for an existing player
    player = get_player_profile("player1")
    if player:
        print("Retrieved player profile for 'player1':")
        print(player)
    else:
        print("Player 'player1' not found.")
    
    # Attempt to retrieve a profile for a non-existing player
    player = get_player_profile("nonexistent_player")
    if player:
        print("Retrieved player profile for 'nonexistent_player':")
        print(player)
    else:
        print("Player 'nonexistent_player' not found.")

    print("\nPlayer profile retrieval testing completed.")

if __name__ == "__main__":
    main()

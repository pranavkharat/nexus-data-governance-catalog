import snowflake.connector
import os
from dotenv import load_dotenv
import getpass

load_dotenv()

print("🔑 Testing Programmatic Access Token as Password")
print("=" * 50)

# Your PAT token
pat_token = "eyJraWQiOiI0NTExODI0NDg5ODg5Nzk4IiwiYWxnIjoiRVMyNTYifQ.eyJwIjoiMjY4OTI1OTQwOjY4ODQ0OTgwMzU3IiwiaXNzIjoiU0Y6MTA0OSIsImV4cCI6MTc2MTY5MjkzNH0.S8ieDPQYef4bsddX8EEZXtXgxf5FXyckO8P-RLvlcT_xfEQoCBFlCT1d4u1v6VBaLDQMc-Y7BDYURHpJ4prbkQ"

# Also get regular password if available
regular_password = input("Enter your regular password (or press Enter to skip): ").strip()

configs_to_test = [
    {
        "name": "PAT as password",
        "params": {
            'account': 'SFEDU02-UUB90967',
            'user': 'CHIPMUNK',
            'password': pat_token  # Use PAT as password
        }
    },
    {
        "name": "PAT with PUBLIC role",
        "params": {
            'account': 'SFEDU02-UUB90967',
            'user': 'CHIPMUNK',
            'password': pat_token,
            'role': 'PUBLIC',
            'database': 'SNOWFLAKE_SAMPLE_DATA'
        }
    },
    {
        "name": "PAT with TRAINING_ROLE",
        "params": {
            'account': 'SFEDU02-UUB90967',
            'user': 'CHIPMUNK',
            'password': pat_token,
            'role': 'TRAINING_ROLE',
            'warehouse': 'ANIMAL_TASK_WH',
            'database': 'TRAINING_DB'
        }
    }
]

# If regular password provided, test it too
if regular_password:
    configs_to_test.append({
        "name": "Regular password + authenticator",
        "params": {
            'account': 'SFEDU02-UUB90967',
            'user': 'CHIPMUNK',
            'password': regular_password,
            'authenticator': 'snowflake'  # Explicitly use password auth
        }
    })

for config in configs_to_test:
    print(f"\n📝 Testing: {config['name']}")
    print("-" * 40)
    
    try:
        conn = snowflake.connector.connect(**config['params'])
        print("✅ SUCCESS! Connected!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
        result = cursor.fetchone()
        print(f"  User: {result[0]}")
        print(f"  Role: {result[1]}")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 WORKING CONFIG: {config['name']}")
        print("Use these parameters in your .env file")
        break
        
    except Exception as e:
        error_msg = str(e)
        if "Failed to authenticate" in error_msg:
            print(f"❌ Authentication failed")
        elif "Network policy" in error_msg:
            print(f"❌ Network policy blocking")
        elif "Invalid OAuth" in error_msg:
            print(f"❌ Token not valid for this auth method")
        else:
            print(f"❌ Error: {error_msg[:100]}")

print("\n" + "=" * 50)
print("If none worked, you might need to generate a new PAT")
print("Or use a different authentication method")
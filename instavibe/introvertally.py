from vertexai import agent_engines
from dotenv import load_dotenv
import pprint
import json 
import os

load_dotenv()

ORCHESTRATE_AGENT_ID = os.environ.get('ORCHESTRATE_AGENT_ID',"projects/789872749985/locations/us-central1/reasoningEngines/7321524784058073088")

agent_engine = agent_engines.get(ORCHESTRATE_AGENT_ID)


#REPLACE ME call_agent_for_plan
#REPLACE ME post_plan_event


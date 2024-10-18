from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
import pandas as pd

# Dataset path
ANIMAL_REGISTRY_DATA_FILE = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/files/ANA-2024-09.csv"
BREED_MAPPING_FILE = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/files/breed_mapping.csv"
TRANSLATION_FILE = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/files/translations.csv"

# Load the datasets
mapping_df = pd.read_csv(TRANSLATION_FILE)
breed_mappings = pd.read_csv(BREED_MAPPING_FILE)
# Load the CSV file and specify the dtype for MatricolaSoggetto
# conversion to scientific notation is occurring when reading the CSV file
animal_df = pd.read_csv(ANIMAL_REGISTRY_DATA_FILE, dtype={'MatricolaSoggetto': str})

# Create a dictionary from the mapping DataFrame
mapping_dict = dict(zip(mapping_df['italian'], mapping_df['english']))

# type conversion
animal_df['idAnimale'] = animal_df['idAnimale'].astype(str).str.strip()


class ActionGetAnimalInfo(Action):

    def name(self) -> Text:
        return "action_get_animal_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        animal_id = tracker.get_slot("AnimalId")

        if animal_id:
            # Create a slice of the DataFrame and make an explicit copy
            # Filter the data for the requested animal ID
            animal_data = animal_df[animal_df["idAnimale"] == str(animal_id)].copy()

            # Combine giorno, mese, and anno into a new column 'Analysis Date'
            animal_data['Analysis Date'] = pd.to_datetime(
                animal_data[['anno', 'mese', 'giorno']].rename(
                    columns={'anno': 'year', 'mese': 'month', 'giorno': 'day'})
            )

            # Convert to date type to remove the time component
            animal_data['Analysis Date'] = animal_data['Analysis Date'].dt.date

            # Remove leading zeros from breed codes
            animal_data['codiceRazzaAIA'] = animal_data['codiceRazzaAIA'].str.lstrip('0')  # animal
            animal_data['RazzaPadre'] = animal_data['RazzaPadre'].str.lstrip('0')  # sire
            animal_data['RazzaMadreGenetica'] = animal_data['RazzaMadreGenetica'].str.lstrip('0')  # dam

            animal_data = animal_data.merge(
                breed_mappings[['BreedAIACode', 'BreedCodeAIASpecies', 'BreedName']],
                left_on=['codiceRazzaAIA', 'codiceSpecieAIA'],
                right_on=['BreedAIACode', 'BreedCodeAIASpecies'],
                how='left'
            ).rename(columns={'BreedName': 'Breed'})

            # Merge to get the sire breed name
            animal_data = animal_data.merge(
                breed_mappings[['BreedAIACode', 'BreedName']],
                left_on=['RazzaPadre'],
                right_on=['BreedAIACode'],
                how='left'
            ).rename(columns={'BreedName': 'Sire Breed'})

            # Merge to get the sire breed name
            animal_data = animal_data.merge(
                breed_mappings[['BreedAIACode', 'BreedName']],
                left_on=['RazzaMadreGenetica'],
                right_on=['BreedAIACode'],
                how='left'
            ).rename(columns={'BreedName': 'Dam Breed'})

            # Drop the duplicate columns
            animal_data.drop(
                ['BreedAIACode', 'BreedAIACode_x', 'BreedAIACode_y', 'BreedCodeAIASpecies', 'RazzaSoggetto', 'Specie',
                 'BreedAIACode', 'codiceRazzaAIA', 'RazzaMadreGenetica', 'RazzaPadre', 'anno', 'mese', 'giorno'],
                axis=1, inplace=True)

            if not animal_data.empty:
                # Rename the columns using the mapping dictionary
                animal_data.rename(columns=mapping_dict, inplace=True)

                info_list = ["<ul>"]  # Start the unordered list

                for index, row in animal_data.iterrows():
                    for key, value in row.items():
                        # Check if the value is not NaN and not an empty string
                        if pd.notna(value) and value != "":
                            info_list.append(f"<li>{key}: {value}</li>")  # Append each item as a list item

                info_list.append("</ul>")  # Close the unordered list

                # Join the list to form a single string
                info_list_str = "\n".join(info_list)

                dispatcher.utter_message(text=f"Here is the information for animal {animal_id}: {info_list_str}")
            else:
                dispatcher.utter_message(text="No data found for the given animal ID.")
        else:
            dispatcher.utter_message(text="Please provide a valid Animal ID.")

        return []


class ActionGetAge(Action):

    def name(self) -> Text:
        return "action_get_age"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        animal_id = tracker.get_slot("AnimalId")
        if animal_id:
            animal_data = df[df["idAnimale"] == animal_id]
            if not animal_data.empty:
                birth_date = pd.to_datetime(animal_data.iloc[0]["DataNascita"])
                current_age = pd.Timestamp.now().year - birth_date.year
                dispatcher.utter_message(text=f"The animal with ID {animal_id} is {current_age} years old.")
            else:
                dispatcher.utter_message(text="No data found for the given animal ID.")
        else:
            dispatcher.utter_message(text="Please provide a valid Animal ID.")

        return []


class ActionGetRegionalData(Action):

    def name(self) -> Text:
        return "action_get_regional_data"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        # Example logic to fetch regional data based on entities
        codice_istat = tracker.get_slot("codiceIstat")
        sigla_provincia = tracker.get_slot("siglaProvincia")

        # Implement your logic to retrieve regional data here
        if codice_istat and sigla_provincia:
            # Replace the following line with actual data fetching logic
            data = f"Regional data for ISTAT code {codice_istat} in {sigla_provincia}"
            dispatcher.utter_message(text=data)
        else:
            dispatcher.utter_message(text="I couldn't find regional data for the provided details.")

        return []


class ActionGetBreedingInfo(Action):

    def name(self) -> Text:
        return "action_get_breeding_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        # Example logic to fetch breeding information based on slots
        codice_specie_aia = tracker.get_slot("codiceSpecieAIA")
        codice_razza_aia = tracker.get_slot("codiceRazzaAIA")

        # Implement your logic to retrieve breeding information here
        if codice_specie_aia and codice_razza_aia:
            # Replace this line with actual data fetching logic
            breeding_info = f"Breeding information for species code {codice_specie_aia} and breed code {codice_razza_aia}."
            dispatcher.utter_message(text=breeding_info)
        else:
            dispatcher.utter_message(text="I couldn't find breeding information for the provided details.")

        return []

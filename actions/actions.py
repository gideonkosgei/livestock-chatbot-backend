from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
import pandas as pd
import glob
import os
import zipfile

# Dataset path
BREED_MAPPING_FILE = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/datasets/breed_mapping.csv"
TRANSLATION_FILE = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/datasets/translations.csv"
PROVINCE_MAPPING_FILE =  "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/datasets/provinces_mapping.csv"
SPECIES_MAPPING_FILE = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/datasets/species_mapping.csv"
ANIMAL_REGISTRY_DIR = "/home/kosgei/dev/personal/chatbots/livestock_chatbox/rasa/datasets/animal_registry/"


class ActionGetAnimalInfo(Action):

    def name(self) -> Text:
        return "action_get_animal_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        animal_id = tracker.get_slot("AnimalId")
        animal_df = load_animal_registry_data(ANIMAL_REGISTRY_DIR)

        if animal_id:
            # Create a slice of the DataFrame and make an explicit copy
            # Filter the data for the requested animal ID
            animal_data = animal_df[animal_df["Animal ID"] == str(animal_id)]

            if not animal_data.empty:
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


def load_animal_registry_data(path):
    # Use glob to find all zip files in the specified directory
    zip_files = glob.glob(os.path.join(ANIMAL_REGISTRY_DIR, "*.zip"))

    # Load the datasets
    mapping_df = pd.read_csv(TRANSLATION_FILE)
    breed_mappings = pd.read_csv(BREED_MAPPING_FILE)
    province_mappings = pd.read_csv(PROVINCE_MAPPING_FILE)
    species_mappings = pd.read_csv(SPECIES_MAPPING_FILE)
    mapping_dict = dict(zip(mapping_df['italian'], mapping_df['english']))

    # Initialize a list to hold DataFrames
    dataframes_animal = []
    # Define the desired data types for specific columns
    dtype_dict = {
        'MatricolaSoggetto': str,
        'idMisuraPrimaria': str,
        'idAnimale': str,
        'codiceRazzaAIA': str,
        'RazzaSoggetto': str,
        'RazzaMadreGenetica': str,
        'RazzaPadre': str,
        'NomeSoggetto': str,
        'DataApplicazioneMarca': str,
        'NatoDaEmbriotransfer': str,
        'TipoOrigine': str,
        'SessoSoggetto': str,
        'Specie': str
    }

    # Loop through the list of zip files and read them into DataFrames
    for i, zip_file in enumerate(zip_files):
        try:
            with zipfile.ZipFile(zip_file, 'r') as z:
                # Get the list of CSV files in the zip
                csv_files = z.namelist()
                # Assuming there is only one CSV file per zip, you can adjust this as needed
                for csv_file in csv_files:
                    with z.open(csv_file) as f:
                        df = pd.read_csv(f, dtype=dtype_dict)
                        # Append the DataFrame to the list
                        dataframes_animal.append(df)

        except Exception as e:
            print(f"Error reading {zip_file}: {e}")

    # Concatenate all DataFrames into a single DataFrame vertically
    animal_df = pd.concat(dataframes_animal, ignore_index=True)

    # Enrich dataset
    # Combine giorno, mese, and anno into a new column 'Analysis Date'
    animal_df['Analysis Date'] = pd.to_datetime(
        animal_df[['anno', 'mese', 'giorno']].rename(
            columns={'anno': 'year', 'mese': 'month', 'giorno': 'day'})
    )

    # Convert to date type to remove the time component
    animal_df['Analysis Date'] = animal_df['Analysis Date'].dt.date

    # List of columns to remove leading zeros from
    columns_to_strip = ['codiceRazzaAIA', 'RazzaPadre', 'RazzaMadreGenetica']

    # Remove leading zeros for each specified column
    for column in columns_to_strip:
        animal_df[column] = animal_df[column].str.lstrip('0')

    # Merge to  get animal breed name
    animal_df = animal_df.merge(
        breed_mappings[['BreedAIACode', 'BreedCodeAIASpecies', 'BreedName']],
        left_on=['codiceRazzaAIA', 'codiceSpecieAIA'],
        right_on=['BreedAIACode', 'BreedCodeAIASpecies'],
        how='left'
    ).rename(columns={'BreedName': 'Breed'})

    # Merge to get the sire breed name
    animal_df = animal_df.merge(
        breed_mappings[['BreedAIACode', 'BreedName']],
        left_on=['RazzaPadre'],
        right_on=['BreedAIACode'],
        how='left'
    ).rename(columns={'BreedName': 'Sire Breed'})

    # Merge to get the sire breed name
    animal_df = animal_df.merge(
        breed_mappings[['BreedAIACode', 'BreedName']],
        left_on=['RazzaMadreGenetica'],
        right_on=['BreedAIACode'],
        how='left'
    ).rename(columns={'BreedName': 'Dam Breed'})

    # Merge to get province name
    animal_df = animal_df.merge(
        province_mappings[['abbreviation', 'name']],
        left_on=['siglaProvincia'],
        right_on=['abbreviation'],
        how='left'
    ).rename(columns={'name': 'Province'})

    # Merge to get province name
    animal_df = animal_df.merge(
        species_mappings[['code', 'species','species_category']],
        left_on=['codiceSpecieAIA'],
        right_on=['code'],
        how='left'
    ).rename(columns={'species_category': 'Species Category'})

    # Drop the duplicate/unwanted columns
    animal_df.drop(
        ['BreedAIACode', 'BreedAIACode_x', 'BreedAIACode_y', 'BreedCodeAIASpecies', 'RazzaSoggetto', 'Specie','codiceIstat',
         'RazzaMadreGenetica', 'RazzaPadre', 'anno', 'mese', 'giorno','abbreviation','code'],
        axis=1, inplace=True)

    # Rename the columns using the mapping dictionary
    animal_df.rename(columns=mapping_dict, inplace=True)

    return animal_df

from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
import pandas as pd
import glob
import os
import zipfile
import matplotlib.pyplot as plt
import base64
from io import BytesIO

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

        animal_id = tracker.get_slot("animal_id")
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

        animal_id = tracker.get_slot("animal_id")
        animal_df = load_animal_registry_data(ANIMAL_REGISTRY_DIR)
        if animal_id:
            animal_data = animal_df[animal_df["Animal ID"] == animal_id]
            if not animal_data.empty:
                birth_date = pd.to_datetime(animal_data.iloc[0]["Date Of Birth"])
                current_date = pd.Timestamp.now()
                age_in_months = (current_date.year - birth_date.year) * 12 + (current_date.month - birth_date.month)
                message = f"The animal with ID {animal_id} was born on {birth_date.date()} and is currently {age_in_months} month(s) old."
                dispatcher.utter_message(text=message)

            else:
                dispatcher.utter_message(text="No data found for the given animal ID.")
        else:
            dispatcher.utter_message(text="Please provide a valid Animal ID.")

        return []


class ActionProvideBreedDistribution(Action):

    def name(self) -> Text:
        return "action_provide_breed_distribution"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        species = tracker.get_slot('species')

        animal_df = load_animal_registry_data(ANIMAL_REGISTRY_DIR)

        if not animal_df.empty:
            # Normalize species data by stripping whitespace and converting to lowercase
            animal_df['species'] = animal_df['species'].str.strip().str.lower()
            species = species.strip().lower()

            # Filter the DataFrame by the specified species
            filtered_df = animal_df[animal_df['species'] == species]

            if filtered_df.empty:
                dispatcher.utter_message(text="Unable to generated breed distribution. No data found for "+species)
            else:

                breed_percentage = filtered_df['Breed'].value_counts(normalize=True) * 100
                # Convert to HTML table
                # html_table = breed_percentage.to_frame(name='Percentage').to_html(classes='table table-bordered',
                #                                                                   header=True, index=True)
                # Plotting the pie chart

                # Show top 10 breeds if there are more than 10, otherwise show all

                message = 'Breed distribution (all breeds)'

                if len(breed_percentage) > 10:
                    message = 'There are too many breeds. For readability, only the top 10 breeds are displayed'
                    breed_percentage = breed_percentage.head(10)

                plt.figure(figsize=(8, 8))
                plt.pie(
                    breed_percentage,
                    labels=breed_percentage.index,
                    autopct='%1.1f%%',
                    startangle=140,
                    wedgeprops={'edgecolor': 'black'}
                )
                plt.title('Breed Distribution')


                # Save the plot to a BytesIO object
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png')
                plt.close()

                # Convert the BytesIO object to a base64 string
                img_buffer.seek(0)
                img_base64 = base64.b64encode(img_buffer.read()).decode('utf-8')
                # Create the HTML image tag
                html_img_tag = f'{message} <br/><br/><img src="data:image/png;base64,{img_base64}" alt="Breed Distribution">'

                dispatcher.utter_message(text=html_img_tag)
        else:
            dispatcher.utter_message(text="Unable to generated breed distribution")

        return []


class ActionShowSpecies(Action):

    def name(self) -> Text:
        return "action_show_species"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        animal_df = load_animal_registry_data(ANIMAL_REGISTRY_DIR)

        if not animal_df.empty:
            unique_breeds = animal_df['species'].unique()

            # Formatting the unique breeds as an HTML unordered list
            html_breed_list = "<ul>" + "".join([f"<li>{species}</li>" for species in unique_breeds]) + "</ul>"

            message  = "Which species would you like to see the breed distribution for?" + html_breed_list
            dispatcher.utter_message(text=message)
        else:
            dispatcher.utter_message(text="Unable to show breeds")

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

    # Remove rows where 'species' is NaN
    animal_df = animal_df.dropna(subset=['species'])

    return animal_df

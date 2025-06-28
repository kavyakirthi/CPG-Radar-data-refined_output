import pandas as pd
import json
import logging

# Setting up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_json_file(filepath):
    """
    Open a JSON file and return its contents as a Python object.
    """
    try:
        with open(filepath, mode="r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        raise
    except json.JSONDecodeError as exc:
        logging.error(f"JSON decode error in {filepath}: {exc}")
        raise

def fetch_inputs(movies_path, genres_path, writers_path, ref_output_path):
    """
    Loads all the required input files for processing the data.
    """
    movies = pd.read_csv(movies_path, dtype={"year": str})
    genres = load_json_file(genres_path)
    writers = load_json_file(writers_path)
    ref_output = load_json_file(ref_output_path)
    return movies, genres, writers, ref_output

def yearly_avg_rating(df):
    """
    Calculating the mean imdb rating data for every year.
    """
    return df.groupby("year")["imdb_rating"].mean().round(2).to_dict()

def rating_category(difference):
    """
    Utilizing the difference to determine the rating category.
    """
    if difference > 0.5:
        return "Above Average"
    if difference < -0.5:
        return "Below Average"
    return "Average"

def format_movie(row, valid_writer_ids, extra_genres_map, avg_rating_map):
    """
    Formats a movie row into the required output structure.
    """
    year = row["year"]
    rating = float(row["imdb_rating"])
    avg = avg_rating_map.get(year, 0)
    diff = round(rating - avg, 2)
    cat = rating_category(diff)

    # Handling writers
    writer_ids = row.get("writter_id", "").split(",")
    writer_names = row.get("writter_name", "").split(",")
    if len(writer_ids) != len(writer_names):
        logging.warning(f"Writer ID/name mismatch (ID: {row.get('id')})")
        writers = []
    else:
        writers = [
            {
                "id": wid.strip(),
                "name": wname.strip(),
                "valid": wid.strip() in valid_writer_ids
            }
            for wid, wname in zip(writer_ids, writer_names)
        ]

    # Handling cast
    cast_ids = row.get("cast_id", "").split(",")
    cast_names = row.get("cast_name", "").split(",")
    if len(cast_ids) != len(cast_names):
        logging.warning(f"Cast ID/name mismatch (ID: {row.get('id')})")
        cast = []
    else:
        cast = [
            {"id": cid.strip(), "name": cname.strip()}
            for cid, cname in zip(cast_ids, cast_names)
        ]

    return {
        "rank": int(row["rank"]),
        "id": row["id"],
        "name": row["name"],
        "year": int(year),
        "imbd_votes": int(row["imdb_votes"]),
        "imdb_rating": rating,
        "certificate": row["certificate"],
        "duration": int(row["duration"]),
        "genre": row["genre"],
        "img_link": row["img_link"],
        "cast": cast,
        "director": {
            "id": row["director_id"],
            "name": row["director_name"]
        },
        "writers": writers,
        "extra_genres": extra_genres_map.get(row["id"], []),
        "rating_percentage": round(rating * 10, 1),
        "popularity_score": round(rating * int(row["imdb_votes"]) / 1000, 2),
        "duration_hours": round(int(row["duration"]) / 60, 2),
        "release_date": f"{year}-01-01",
        "reception": {
            "year_average_rating": avg,
            "rating_difference": diff,
            "category": cat
        }
    }

def build_movie_list(dataframe, genre_lookup, writer_list, yearly_avg):
    """
    Iterating through each movie record and constructing a list of movie dictionaries in the required format.
    """
    movies_collection = []
    for _, record in dataframe.iterrows():
        try:
            movie_dict = format_movie(record, writer_list, genre_lookup, yearly_avg)
            movies_collection.append(movie_dict)
        except Exception as error:
            logging.error(f"Unable to process movie with ID {record.get('id')}: {error}")
    return movies_collection

def compare_outputs(generated, reference):
    """
    Confirming that the produced output data corresponds to the intended reference output.
    """
    return generated == reference

def main_process(
    movie_file="movies.csv",
    genre_file="genre.json",
    writer_file="writters.json",
    expected_output_file="desired_output.json",
    output_file="output_refined.json"
):
    """
    Handles the workflow of reading input files, transforming data, and saving the processed movie information.
    """
    # Read all input data
    movie_data, genre_data, writer_data, expected_output = fetch_inputs(
        movie_file, genre_file, writer_file, expected_output_file
    )

    # Step 2: Build lookup dictionaries
    genre_lookup = {entry["id"]: entry["extra_genres"] for entry in expected_output}
    yearly_average = yearly_avg_rating(movie_data)

    # Step 3: Process and generate the movie output
    processed_movies = build_movie_list(movie_data, genre_lookup, writer_data, yearly_average)

    # Step 4: Write the result to a file
    with open(output_file, "w", encoding="utf-8") as out_file:
        json.dump(processed_movies, out_file, indent=2)
    logging.info(f"Output successfully written to {output_file}")

    # Step 5: Check if the output matches the expected reference
    if compare_outputs(processed_movies, expected_output):
        logging.info("Output matches the expected reference data.")
    else:
        logging.warning("Output does not match the expected reference data.")

if __name__ == "__main__":
    main_process()
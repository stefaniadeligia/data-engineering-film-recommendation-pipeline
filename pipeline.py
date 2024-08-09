import luigi
import subprocess

class ProcessFilmData(luigi.Task):
    input_file = luigi.Parameter(default='resources/csv/allFilms.csv')
    schema_file = luigi.Parameter(default='resources/json/allFilesSchema.json')
    output_folder = luigi.Parameter(default='output')

    def run(self):
        #Run stage1 as a subrocess
        subprocess.run([
            'python', 'stage1.py',
            self.input_file,
            self.schema_file,
            self.output_folder
        ], check=True)

    def output(self):
        return luigi.LocalTarget(f"{self.output_folder}/films.parquet")

class ProcessGenres(luigi.Task):
    input_path = luigi.Parameter(default='output')
    genres_folder = luigi.Parameter(default='genres')

    def requires(self):
        return ProcessFilmData()

    def run(self):
        #Run stage2 as a subrocess
        subprocess.run([
            'python', 'stage2.py',
            self.input_path,
            self.genres_folder
        ], check=True)

    def output(self):
        # List all parquet files in the genres_folder
        output_files = [
            luigi.LocalTarget(os.path.join(self.genres_folder, f))
            for f in os.listdir(self.genres_folder)
            if f.endswith('.parquet')
        ]
        return output_files

if __name__ == "__main__":
    luigi.run()

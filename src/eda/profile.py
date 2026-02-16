from ydata_profiling import ProfileReport
from config.settings import DATA_OUTPUTS

def generate_profile(dataset_name, df):
    profile = ProfileReport(df, title=f"{dataset_name} Profiling")
    output_file = DATA_OUTPUTS / f"{dataset_name}_profile.html"
    profile.to_file(output_file)

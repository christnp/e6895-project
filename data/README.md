# Tips for data collection

## Google Bucket
Most data is stored as a CSV file in a Google Bucket. The code has been devloped
such that the data is stored as a signle CSV file ratehr than multiple CSV files
(i.e., we use .repartition(1) to bring all data to single worker). This makes
it much easier to postprocess the data.

The first step is to get the data from the target bucket. This can easily be
done from your local development machine using the `gsutil` function. For example,
if I'm trying to copy the CSV files stored in my `_multilayer` output directory
to my current path, I'd simply type the following in the terminal:
```
gsutil -m cp -r gs://eecs-e6895-bucket/output/_multilayer/20200514095202/* . 
```
Now that the data is copied to your local machine, you will notice that the actual
CSV file is located beneath a subdirectory that has the name as identified in the 
code. That is, if I tell my code to store the data as 'imafile.csv' then the data
in the bucket would actually be saved as `imafile.csv/123abc456xyz.csv'. This is
very standard for Google Dataproc, but is not necessarily how I like to view
my data -- so I wrote a python script that recursively goes through every directory
ending with `.csv`, copies the name of the subdirectory and renames the file (with
a prefix) accordingly, moves the file out of the subdirectory, removes the subdirectory,
removes the prefix from the file. Bingo!

```python
 files_in_path = os.listdir(self.csv_path)
        csv_paths = []
        for file in files_in_path:
            if file.endswith(".csv"):
                csv_paths.append(os.path.join(self.csv_path, file))

        for path in csv_paths:
            name = os.path.basename(path)
            try:
                for file in os.listdir(path):
                    if file.endswith(".csv"):
                        os.rename(os.path.join(path, file), os.path.join(self.csv_path, "moved_"+name))
            except Exception as e:
                print("Failed to rename file in path {}: {}".format(path,e))
        
        for file in files_in_path:
            file_path = os.path.join(self.csv_path,file)
            if file.endswith(".csv"):
                if not file.startswith("moved_"):
                    try:
                        shutil.rmtree(file_path)
                    except Exception as e:
                        print("Failed to remove direcotry {}: {}".format(file_path,e))
                if file.startswith("moved_"):
                    try:
                        os.rename(file_path, file_path.replace('moved_', ''))
                    except Exception as e:
                        print("Failed rename file {}: {}".format(file_path,e))

```
\* sorry it's not commented and it could probably by a lot more 'pythonic'
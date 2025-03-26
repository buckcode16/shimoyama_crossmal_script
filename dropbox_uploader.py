#!/usr/bin/env python3
import os
import datetime
import dropbox
from dropbox.files import WriteMode

def upload_file_to_dropbox(local_file, dropbox_destination, access_token):
    """
    Uploads the specified local file to Dropbox with a timestamp appended to the filename,
    so that each upload creates a new file instead of replacing an existing one.
    
    Parameters:
      local_file (str): Path to the local file to upload.
      dropbox_destination (str): The destination path in Dropbox (e.g., "/Folder/file.xml").
      access_token (str): Dropbox API access token.
    
    The function inserts a timestamp before the file extension.
    For example, if dropbox_destination is "/Reports/output.xml" and the current time is
    2023-04-20 15:30:45, the file will be uploaded to:
      /Reports/output_20230420_153045.xml
    """
    # Initialize Dropbox client
    dbx = dropbox.Dropbox(access_token)
    
    # Read the local file content
    with open(local_file, 'rb') as f:
        file_content = f.read()
    
    # Split the destination into base and extension
    base, ext = os.path.splitext(dropbox_destination)
    # Create a timestamp string
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    # Construct the new destination path with the timestamp
    new_destination = f"{base}_{timestamp}{ext}"
    
    try:
        dbx.files_upload(file_content, new_destination, mode=WriteMode('add'))
        print(f"Successfully uploaded {local_file} to {new_destination}")
    except Exception as e:
        print(f"Failed to upload {local_file} to Dropbox: {e}")
        raise

# if __name__ == "__main__":
#     LOCAL_FILE = ".xml"
#     DROPBOX_DEST = "/Reports/output.xml"
#     ACCESS_TOKEN = "sl.u.AFns-FdsZevXHZoQSfHJOA9u2XpwrMLXxzPuiYSIFKtYR9hpzpGrsLC02zCYmFokVPt55MTCKbeyi0uefgenC5GP2ujrkbJXs7e8WjSko1SW44Ds1XRqCAsG26uks--_Zrpge0bmHcFHs10YTDgd107DU1H3pQGaSFRpFIMMaYXZmuBTauJDDFtNwWlJCrfprldDuT8BUJ-FhjlT0d1dqeFjhotqjOcLMtwc_2v3C4Jrx5yvgL0rBWWejfQ3nGsv9w7XrgMu3uK_ELekj4wuZMnEYpnwihacGTQaLYFkVHb7KGszW_yDH6J7sx_pQMSmWjqD6SUZ9a5yEnyOOUmqUY55INr5J-WLY3yPAc-SXftrezCaM-xdf_-x1aNfH8ZjQYEnQTR3GhRRvTeaZVyYSpim7O1dl4_28WVqSMvQYfKs2rI7dFJrYK5KK_yYmjGr6TIMnYxNQTPdKlm0aRDXl8JaX2p6P1FSXnaOCX1M92uuXjIvYP34JlTnVv1yrG29u_iq5uRUzRYFl-nvCREQmOL4kEHjTpijuqQynNGX7cZaOCzf81hVSul0t5iO1SX8gcw2A3GyAiBKsiLqGmRxX0CJ17GxkesnAEOUF-YRs5htRhQ6O5aeT6rEmeRa9omiD0FrybDM8Ynjn6siIGe7FUg3_XRBEYiDgqa-CoRXaKJE5uJKHN0g5K4qjx5IFVPrNKjJhrfQsSxBOrT1becis9GPP6LXW5c3Y46xQeOr0CeME-ct19zzoZauOBS_0O78bGeSCdzSMZyU56HBFU76nooqaawr1NNyPBGq9C7GclLUe55MJs1lH4Eg1Hc-7IhLA5QiS1YJ1U3OEYAPBJW7DFsOGCWumOKLaHgp5uZ3Vfzno_WzHwuUd8cPueHU8avJuBLzDnec5o2B5phqwtwFPECoFe4a6_AsmWP9IF1s4R-APSS78PjKT95pDcxju4MZYTFPBAleD71roFrSchwH0YpXN8Yp_7_I4KtOet67nc2Kztp-08Kdn2pYA9g9sm_P9GH-I9WHVSyyzBRqYeO_d1qqsCFkFV5xoI65fGDZJoc4J5tp1ZD6tYEBa8FvTj15UfwcGCaBq9Q6uzvBE48G9kGgccQhKaMJFyXOKiDf14ccoiFc8LGUeeNL6lrHyhmmuvvmCe7VG5n7-sNQ7E3VFoga449wOE2314qF7zVeqs7rtJ9uzJ641OhNZYYEQ1xyAoaCSJ283D4qErBtNQEDp8mrCUwROjJE4FOmxi0SZamxAwMyCdJKS9y5oKaOIO_kGQ5Xf7-lyOEgK8KbilTxMtDpxR_x4fSJ3tDdd6ciPflm9g"
    
#     upload_file_to_dropbox(LOCAL_FILE, DROPBOX_DEST, ACCESS_TOKEN)

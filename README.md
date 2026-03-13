# Epstein Photo Network Visualization

## Scripts


**00__extract_pdf_images.sh**: Extracts images from every PDF in specified directory, recursively
**01__dedup_images.sh**: Deletes duplicate image files
**02__downsize_images.sh**: Downsize any image above 5MB
**03__preprocess_faces.py**: Uses local (free) face detection model to filter out images without faces
**04__cluster_faces.py**: Index images with AWS Rekognition
**05__recognize_celebrities.py**: Associate faces with "celebrities" using AWS Rekognition
**06__extract_faces.py**: Write images of extracted faces to file
**07__create_graph.py**: Build network graph based on face co-occurrence in photos, save as GraphML
**08__visualize_graph.py**: Process laid-out graph data into data for D3 network visualization
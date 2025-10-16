#!/usr/bin/env python3
"""
PHASE #2, Step #2

Enhanced Relationship and Generator

Integrated features:
1. Identifies instances with identical hashes and generates bidirectional relationships “bodi:hasSameHashCodeAs”
2. Links creation dates to Records via rico:hasCreationDate using “dcterms:created” metadata
3. Links modification dates to Records via rico:hasModificationDate using “dcterms:modified” metadata
4. Links modification dates to RecordSets via rico:hasModificationDate using “st_mtime” metadata
5. Link modification dates to Instantiations using rico:hasModificationDate with metadata “st_mtime”

Translated with DeepL.com (free version)


"""

import argparse
import json
import logging
import requests
import sys
import time
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from dataclasses import dataclass
from urllib.parse import quote
from collections import defaultdict


MIME_TYPE_CATEGORY_MAPPING = {
    # ==================================================================================
    # 1. VIDEO 
    # ==================================================================================
    'video/mp4': 'Video (MP4)',                    #
    'video/mpeg': 'Video (MPEG)',                
    'video/quicktime': 'Video (QuickTime)',       
    'video/webm': 'Video (WebM)',                
    'video/x-m4v': 'Video (M4V)',                 
    'video/x-ms-wmv': 'Video (WMV)',           
    'video/x-msvideo': 'Video (AVI)',              
    'video/x-matroska': 'Video (MKV)',            
    'video/3gpp': 'Video (3GP)',                 
    'video/x-flv': 'Video (FLV)',                  
    
    # ==================================================================================
    # 2. AUDIO 
    # ==================================================================================
    'audio/aac': 'Audio (AAC)',                    
    'audio/mp4': 'Audio (MP4)',                    
    'audio/mpeg': 'Audio (MP3)',                  
    'audio/ogg': 'Audio (OGG)',                 
    'audio/x-matroska': 'Audio (MKA)',             
    'audio/x-pn-realaudio': 'Audio (RealAudio)',  
    'audio/x-wav': 'Audio (WAV)',                 
    'audio/wav': 'Audio (WAV)',                   
    'audio/flac': 'Audio (FLAC)',                  
    'audio/webm': 'Audio (WebM)',                  
    'audio/x-ms-wma': 'Audio (WMA)',            
    'audio/x-aiff': 'Audio (AIFF)',                
    
    # ==================================================================================
    # 3. IMMAGINI - Formato cruciale per qualità e compatibilità
    # ==================================================================================
    'image/bmp': 'Image (BMP)',               
    'image/gif': 'Image (GIF)',                 
    'image/jpeg': 'Image (JPEG)',               
    'image/pcx': 'Image (PCX)',             
    'image/pict': 'Image (PICT)',               
    'image/png': 'Image (PNG)',                
    'image/svg+xml': 'Vectiorial Image (SVG)',  
    'image/tiff': 'Image (TIFF)',               
    'image/vnd.djvu': 'Image (DjVu)',           
    'image/vnd.fpx': 'Image (FlashPix)',        
    'image/webp': 'Image (WebP)',               
    'image/x-cursor': 'Image (Cursor)',        
    'image/x-icon': 'Image (Icon)',            
    'image/x-jps': 'Image (JPS)',               
    'image/heic': 'Image (HEIC)',               
    'image/heif': 'Image (HEIF)',    

    
    # ==================================================================================
    # 4. DOCUMENTI 
    # ==================================================================================
    'application/msword': 'Document (Word Legacy)',                    
    'application/vnd.ms-word.template.macroEnabledTemplate': 'Template (Word with Macro)',  
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'Document (Word)', 
    'application/vnd.openxmlformats-officedocument.wordprocessingml.template': 'Template (Word)',   
    'application/vnd.oasis.opendocument.text': 'Document (OpenDocument)',    
    'application/pdf': 'Document (PDF)',                             
    'text/html': 'Document Web (HTML)',                             
    'text/plain': 'Document (Text)',                               
    'text/rtf': 'Document (RTF)',                                    
    
    # ==================================================================================
    # 5. FOGLI DI CALCOLO 
    # ==================================================================================
    'application/vnd.ms-excel': 'Spreadsheet (Excel Legacy)',      
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'Spreadsheet (Excel)',  
    'application/vnd.oasis.opendocument.spreadsheet': 'Spreadsheet (OpenDocument)',  

    # ==================================================================================
    # 6. PRESENTAZIONI 
    # ==================================================================================
    'application/vnd.ms-powerpoint': 'Presentation (PowerPoint Legacy)',  
    'application/vnd.ms-officetheme': 'Theme (Office)',                     
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'Presentation (PowerPoint)',  
    'application/vnd.oasis.opendocument.presentation': 'Presentation (OpenDocument)',  
    
    # ==================================================================================
    # 7. E-BOOK 
    # ==================================================================================
    'application/epub+zip': 'E-Book (EPUB)',                          
    'application/x-mobipocket-ebook': 'E-Book (Mobipocket)',          
    
    # ==================================================================================
    # 8. ARCHIVI 
    # ==================================================================================
    'application/zip': 'Archive (ZIP)',                              
    'application/x-7z-compressed': 'Archive (7Z)',                   
    'application/x-rar-compressed': 'Archive (RAR)',                 
    'application/bzip2': 'Archive (BZip2)',                          
    'application/x-gzip': 'Archive (GZip)',                          
    'application/gzip': 'Archive (GZip)',                            
    'application/x-tar': 'Archive (TAR)',                            
    
    # ==================================================================================
    # 9. FONT 
    # ==================================================================================
    'font/woff': 'Font (WOFF)',                                       
    'font/woff2': 'Font (WOFF2)',                                     
    'application/x-font-ttf': 'Font (TrueType)',                      
    'application/font-woff': 'Font (WOFF)',                           
    'application/font-woff2': 'Font (WOFF2)',                         
    
    # ==================================================================================
    # 10. DATI STRUTTURATI
    # ==================================================================================
    'application/json': 'Data (JSON)',                                
    'application/xml': 'Data (XML)',                                  
    'application/rdf+xml': 'Semantic data (RDF)',                   
    'text/xml': 'Data (XML)',                                       
    'text/csv': 'Table (CSV)',                                      
    
    # ==================================================================================
    # 11. FILE DI SISTEMA 
    # ==================================================================================
    'application/vnd.iccprofile': 'Color Profile (ICC)',             
    'application/x-iso9660-image': 'Disk Image (ISO)',            
    
    # ==================================================================================
    # 12. APPLICAZIONI/SOFTWARE
    # ==================================================================================
    'application/x-shockwave-flash': 'Application (Flash)',          
    'application/vnd.adobe.air-application-installer-package+zip': 'Application (Adobe AIR)',  
    'application/x-msdownload': 'Application (Windows)',             
    'application/x-executable': 'Application (Eseguibile)',          
    
    # ==================================================================================
    # 13. PROTOCOLLI DI RETE
    # ==================================================================================
    'application/x-bittorrent': 'Torrent File (BitTorrent)',          
    
    # ==================================================================================
    # 14. STAMPA/OUTPUT 
    # ==================================================================================
    'application/postscript': 'Print Document (PostScript)',        
    
    # ==================================================================================
    # 15. FILE SCONOSCIUTI 
    # ==================================================================================
    'application/unknown': 'Unknown file',                      
    'application/ResEdit': 'Resource (ResEdit)',                       
    'application/octet-stream': 'Binary File (Generic)',    
    
    # ==================================================================================
    # 16. FILE DI POSTA
    # ==================================================================================
    'application/vnd.ms-outlook-pst': 'Outlook Data File (PST)',                    
}


BASE_URIS = {
    'date': "http://ficlit.unibo.it/ArchivioEvangelisti/date_",
    'technical_metadata_set': "http://ficlit.unibo.it/ArchivioEvangelisti/technical_metadata_set_",
    'title': "http://ficlit.unibo.it/ArchivioEvangelisti/title_"  # se crea Title entities
}

# === FIELD CLASSIFICATION FOR TECHNICAL METADATA SETS ===
file_system_fields = {
    "FileName", "Directory", "FileSize", "FileModifyDate", "FileAccessDate",
    "FileInodeChangeDate", "FilePermissions", "FileAttributes", "AccessDate",
    "TargetFileSize", "File Modified_Date", 
    "File Name", "File Size", "st_size", "st_mtime", "st_ctime", "st_atime",
    "st_mode", "st_uid", "st_gid", "file_type", "st_nlink", "st_ino", "st_dev",
    "st_blksize", "st_blocks"
}

document_content_fields = {
    "Title", "Author", "Subject", "LastModifiedBy", "LastSavedBy", "CreateDate",
    "dcterms:created", "ModifyDate", "dcterms:modified", "LastPrinted", 
    "Content-Type-Parser-Override", "Content-Type-Hint", "meta:print-date", 
    "RevisionNumber", "cp:revision", "Software", "extended-properties:Application", 
    "Template", "extended-properties:Template", "Company", "extended-properties:Company", 
    "Manager", "extended-properties:Manager", "Category", "cp:category", "LanguageCode", 
    "Keywords", "meta:keyword", "Characters", "meta:character-count", "CharactersWithSpaces", 
    "Content-Length", "Content-Type", "meta:character-count-with-spaces", "Words", 
    "meta:word-count", "Pages", "meta:page-count", "xmpTPg:NPages", "TotalEditTime", 
    "extended-properties:TotalTime", "ProductVersion", "FileVersion", 
    "InternalVersionNumber", "AppVersion", "Generator", "InternalName", "OriginalFileName", 
    "FileDescription", "LegalCopyright", "meta:last-author", 
    "meta:character-count", "w_Comments", "ContentType", "Comment", "ProductName", 
    "System", "LinksUpToDate", "HeadingPairs", "CompObjUserType", "Encoding", "Newlines",
    "WordCount", "Paragraphs", "dc:creator", "dc:title", "language", "TitleOfParts", "Creator", 
    "author", "creator", "dc:subject", "dc:description", 
    "Subject", "Comment", "w:Comments", "date", "JPEG Comment", "CompanyName", "LegalCopyright", "ProductName", "PageCount",
    "LastAuthor", "RevisionNumber", "Application", "AppVersion", "DocumentID",  
    "keywords", "description", "author", "generator", "Content-Language",
    "FontName", "FontFamily", "Designer-en-US", "Copyright-en-US" , "FileType" 
}

image_specific_fields = {
    "tiff:ImageWidth", "tiff:ImageLength", "BitsPerSample", "tiff:BitsPerSample", 
    "Exif IFD0:Compression", "PhotometricInterpretation", "Exif IFD0:Photometric Interpretation",
    "SamplesPerPixel", "Exif IFD0:Samples Per Pixel", "PlanarConfiguration", 
    "Exif IFD0:Planar Configuration", "StripOffsets", "StripByteCounts", "tiff:XResolution", 
    "tiff:YResolution", "tiff:ResolutionUnit", "YCbCrSubSampling", "PhotoshopBGRThumbnail", 
    "PhotoshopThumbnail", "PhotoshopFormat", "PhotoshopQuality", "DCTEncodeVersion", 
    "APP14Flags0", "APP14Flags1", "JFIFVersion", "ExifByteOrder", "Predictor", "ImageSize", 
    "Megapixels", "ImageLength", "HotspotX", "HotspotY", "TransparentColor", 
    "Transparency Alpha", "Transparency TransparentIndex", "WhitePoint", "PrimaryChromaticities", 
    "Thumbnail Data", "Thumbnail Height Pixels", "Thumbnail Width Pixels", "Exif_IFD0_White_Point", 
    "Image Height", "Exif_IFD0_Predictor", "PLTE_PLTEEntry", "tiff_SamplesPerPixel", 
    "Data_PlanarConfiguration", "DCT_Encode_Version", "Exif_IFD0_Unknown_tag_(0x0122)",
    "Exif_IFD0_Unknown_tag_(0x0123)", "Chroma_Gamma", "Exif_IFD0_Strip_Byte_Counts", 
    "Exif_IFD0_New_Subfile_Type", "Chroma_BlackIsZero", "xmp_CreatorTool", 
    "Grid_and_Guides_Information", "Color_Transform", "Exif_IFD0_Image_Height",
    "Color_Transfer_Functions", "Component_1", "Compression_CompressionTypeName", 
    "Exif_IFD0_Resolution_Unit", "Color_Halftoning_Information", "Border_Information", 
    "width", "patches", "ColorMap", "IHDR", "GIFVersion", "Chroma_ColorSpaceType",
    "Grayscale_and_Multichannel_Transfer_Function", "Print_Flags_Information", "Make", 
    "Model", "Flash", "Aperture", "FNumber", "FocalLength", "ExposureProgram",
    "ExposureTime", "ShutterSpeed", "ISO", "WhiteBalance", "MeteringMode",
    "ExifImageHeight", "ExifImageWidth", "ExifVersion", "DateTimeOriginal",
    "Exif SubIFD:Flash", "Exif SubIFD:F-Number", "Exif SubIFD:Focal Length",
    "Exif SubIFD:Exposure Time", "Exif SubIFD:ISO Speed Ratings", "Exif SubIFD:Metering Mode", "Exif SubIFD:White Balance Mode", 
    "Exif IFD0:Make", "Exif IFD0:Model", "Exif IFD0:Orientation", "Exif IFD0:X Resolution", "Exif IFD0:Y Resolution", "Exif IFD0:Date/Time",
    "Interoperability:Interoperability Index", "InteropVersion", "InteropIndex", "Exif Thumbnail:Thumbnail Length", "Exif Thumbnail:Thumbnail Offset",
    "Exif SubIFD:Focal Plane Resolution Unit", "Exif SubIFD:Focal Plane X Resolution", "Exif SubIFD:Focal Plane Y Resolution", "ExposureProgram",            
    "SceneType", "Exif SubIFD:Exposure Program", "PixelUnits", "PixelsPerUnitX", "PixelsPerUnitY", "Dimension HorizontalPixelSize", "Dimension VerticalPixelSize", 
    "CanonModelID", "LensType", "FlashMode", "ShootingMode", "FocusMode", "SequenceNumber", "CameraType", "FirmwareVersion",
    "ICC:Profile Description", "ICC:Color space", "ICC:Profile Size", "ICC:Blue Colorant", "ICC:Green Colorant", "ICC:Red Colorant",
    "pHYs", "tIME", "iTXt", "zTXt", "gAMA", "cHRM", "sBIT", "bKGD", "GPSLatitude", "GPSLongitude", "GPSAltitude", "GPSDateStamp", "GPSTimeStamp"
}

audio_specific_fields = {
    "AudioFormat", "AudioChannels", "channels", "AudioBitsPerSample", "AudioSampleRate", 
    "xmpDM:audioSampleRate", "xmpDM:audioChannelType", "xmpDM:audioSampleType", 
    "AvgBytesPerSec", "Flags", "ID3Size", "Genre", "Album", "Track", "Artist", "Composer", "Year", "Encoder", "LameBitrate", "LameQuality", "LameMethod", "LameStereoMode",
    "xmpDM:artist", "xmpDM:album", "xmpDM:genre", "xmpDM:composer", "MusicCDIdentifier"
}

video_specific_fields = {
    "FrameRate", "VideoBitrate", "VideoFrameRate", "Track2Name", "TrackCreateDate",
    "TrackModifyDate", "TrackID", "TrackDuration", "TrackLayer", "TrackVolume", 
    "MediaHeaderVersion", "MediaCreateDate", "MediaModifyDate", "MediaTimeScale", 
    "MediaLanguageCode", "TrackHeaderVersion", "OpColor", "HandlerDescription", 
    "GraphicsMode", "MatrixStructure", "MajorBrand", "MinorVersion", "CompatibleBrands", 
    "Rotation", "tracks", "PosterTime", "PreferredRate", "SelectionTime", "PreviewTime", 
    "HandlerVendorID", "NextTrackID"
}

email_fields = {
    "Message:From-Email", "Message:To", "Message:Raw-Header:Return-Path", 
    "Message:Raw-Header:Status", "Message:Raw-Header:X-Sender", 
    "Message:Raw-Header:X-Mailer", "Message:Raw-Header:Content-Type", 
    "Message:Raw-Header:Mime-Version", "Message:Raw-Header:Received", 
    "Message:Raw-Header:X-Original-To", "Message:Raw-Header:Message-Id", 
    "Message:Raw-Header:Reply-To", "Message-Cc", "Message:From-Name", 
    "Message:Raw-Header:Subj", "Message-From", "Message-To"
}

executable_fields = {
    "MachineType", "TimeStamp", "ImageFileCharacteristics", "PEType", 
    "LinkerVersion", "CodeSize", "InitializedDataSize", "UninitializedDataSize", 
    "EntryPoint", "OSVersion", "ImageVersion", "SubsystemVersion", "Subsystem", 
    "FileVersionNumber", "ProductVersionNumber", "FileFlags", "FileFlagsMask", 
    "FileOS", "ObjectFileType", "FileSubtype", "machine_machineType", "machine_endian", "machine:platform", 
    "machine:architectureBits", "machine:endian", "machine:machineType"
}

archive_fields = {
    "ZipRequiredVersion", "ZipBitFlag", "ZipCompression", "ZipModifyDate", 
    "ZipCRC", "ZipCompressedSize", "ZipUncompressedSize", "ZipFileName", "Compressed", "CompressionType", "CompressionLevel", 
    "ArchiveComment", "EncryptionMethod", "Password" 
}

security_fields = {
    "Password", "Encryption", "DocSecurity", "Security", "Permissions",
    "access_permission:can_print", "access_permission:can_modify", 
    "access_permission:extract_content", "encrypted", "CertificateInfo",
    "DigitalSignature", "TrustedDocument", "pdf:encrypted", "pdf:hasXFA", 
    "access_permission:assemble_document", "access_permission:can_print_faithful",
    "access_permission:extract_for_accessibility", "access_permission:fill_in_form",
    "access_permission:modify_annotations", "Linearized", "Tagged"
}

# === METADATA TYPE EQUIVALENCES FOR OWL:SAMEAS RELATIONSHIPS ===
# Mappatura delle equivalenze tra TechnicalMetadataType provenienti da fonti diverse
# Ogni lista contiene metadati equivalenti che dovranno essere collegati con owl:sameAs
METADATA_EQUIVALENCES = [
    ["CreateDate", "dcterms:created"],
    ["MediaModifyDate", "dcterms:modified"],
    ["MediaCreateDate", "Media Created Date", "st_birthtime"],
    ["FileModifyDate", "File Modified Date", "st_mtime"],
    ["FileAccessDate", "st_atime"],
    ["Creator", "dc:creator"],
    ["LastModifiedBy", "meta:last-author"],
    ["FileSize", "File Size", "Content-Length", "st_size"],
    ["RevisionNumber", "cp:revision"],
    ["TotalEditTime", "extended-properties:TotalTime"],
    ["Words", "meta:word-count"],
    ["Characters", "meta:character-count"],
    ["Application", "extended-properties:Application"],
    ["MIMEType", "Content-Type"],
    ["Pages", "meta:page-count"],
    ["ImageHeight", "tiff:ImageLength"],
    ["ImageWidth", "Image Width", "tiff:ImageWidth"],
    ["BitsPerSample", "tiff:BitsPerSample"],
    ["ColorComponents", "Number of Components"],
    ["XResolution", "X Resolution"],
    ["YResolution", "Y Resolution"],
    ["Duration", "xmpDM:duration"],
    ["Comment", "xmpDM:logComment"],
    ["AudioSampleRate", "audioSampleRate"],
    ["PDFVersion", "pdf:PDFVersion"],
    ["Title", "dc:title"],
    ["Producer", "pdf:producer"],
    ["CreatorTool", "xmp:CreatorTool"],
    ["DocumentID", "xmpMM:DocumentID"],
    ["FileInodeChangeDate", "st_ctime"],
    ["FilePermissions", "st_mode"],
    ["Compression", "Compression Type"],
    ["ColorSpace", "Exif SubIFD:Color Space"],
    ["Orientation", "Exif IFD0:Orientation", "tiff:Orientation"],
    ["ResolutionUnit", "Resolution Units", "tiff:ResolutionUnit"],
    ["Make", "Exif IFD0:Make", "tiff:Make"],
    ["Model", "Exif IFD0:Model", "tiff:Model"],
    ["Flash", "Exif SubIFD:Flash", "exif:Flash"],
    ["FNumber", "Aperture", "Exif SubIFD:F-Number", "exif:FNumber"],
    ["FocalLength", "Exif SubIFD:Focal Length", "exif:FocalLength"],
    ["ExposureTime", "Exif SubIFD:Exposure Time", "exif:ExposureTime"],
    ["ISO", "Iso"],
    ["WhiteBalance", "Exif SubIFD:White Balance Mode"],
    ["MeteringMode", "Exif SubIFD:Metering Mode"],
    ["ExposureMode", "Exif SubIFD:Exposure Mode"],
    ["ExposureCompensation", "Exif SubIFD:Exposure Bias Value"],
    ["ExifVersion", "Exif SubIFD:Exif Version"],
    ["LanguageCode", "language"],
    ["MIMEEncoding", "Content-Encoding"],
    ["ThumbnailLength", "Exif Thumbnail:Thumbnail Length"],
    ["ThumbnailOffset", "Exif Thumbnail:Thumbnail Offset"],
    ["Subject", "dc:subject"],
    ["Comment", "w:Comments"],
    ["GPSAltitude", "GPS:GPS Altitude"],
    ["GPSAltitudeRef", "GPS:GPS Altitude Ref"],
    ["GPSDateStamp", "GPS:GPS Date Stamp"],
    ["GPSTimeStamp", "GPS:GPS Time-Stamp"],
    ["GPSProcessingMethod", "GPS:GPS Processing Method"],
    ["SubjectDistance", "Exif SubIFD:Subject Distance"],
    ["VideoFrameRate", "Video Frame Rate"],
    ["PixelAspectRatio", "Pixel Aspect Ratio"],
    ["AudioSampleCount", "Audio Sample Count"],
    ["AvgBytesPerSec", "Avg Bytes Per Sec"],
    ["NumChannels", "Num Channels"],
    ["SampleSize", "Sample Size"],
    ["StreamCount", "Stream Count"],
    ["VideoCodec", "Video Codec"],
    ["VideoFrameCount", "Video Frame Count"],
    ["FrameCount", "Frame Count"],
    ["MaxDataRate", "Max Data Rate"],
    ["PhotometricInterpretation", "Exif IFD0:Photometric Interpretation"],
    ["SamplesPerPixel", "Exif IFD0:Samples Per Pixel", "tiff:SamplesPerPixel"],
    ["Composer", "xmpDM:composer"],
    ["FontFamily", "FontFamilyName"],
    ["FontName-en-US", "FontName"],
    ["FontSubfamily-en-US", "FontSubFamilyName"],
    ["DocSecurity", "extended-properties:DocSecurity"],
    ["CFAPattern", "Exif SubIFD:CFA Pattern"],
    ["PlanarConfiguration", "Exif IFD0:Planar Configuration"],
    ["CopyrightNotice", "Copyright Notice"],
    ["StripOffsets", "Exif IFD0:Strip Offsets"],
    ["AudioFormat", "Audio Format"],
    ["MediaLanguageCode", "Media Language Code"],
    ["GraphicsMode", "Graphics Mode"],
    ["Predictor", "Exif IFD0:Predictor"],
    ["Editing-cycles", "editing-cycles"]
]

# MIME TYPE MAPPING
mime_category_map = {
    "image/jpeg": "image", "image/png": "image", "image/tiff": "image", "image/gif": "image",
    "image/bmp": "image", "image/webp": "image", "image/heic": "image", "image/heif": "image",
    "image/svg+xml": "image", "image/x-icon": "image",
    "video/mp4": "video", "video/quicktime": "video", "video/x-msvideo": "video",
    "video/x-matroska": "video", "video/mpeg": "video", "video/webm": "video",
    "video/x-flv": "video", "video/3gpp": "video",
    "audio/mpeg": "audio", "audio/wav": "audio", "audio/x-wav": "audio",
    "audio/ogg": "audio", "audio/flac": "audio", "audio/mp4": "audio",
    "audio/aac": "audio", "audio/webm": "audio", "audio/x-ms-wma": "audio",
    "audio/x-aiff": "audio",
}

def get_mime_type_category(mime_type):
    if not mime_type:
        return None
    mime_type = mime_type.split(';')[0].strip().lower()
    return mime_category_map.get(mime_type)

def get_metadata_set_for_field(field, mime_type=None, debug=False):
    """Get appropriate metadata set for field based on field name and optionally mime type"""
    
    classification_reason = ""
    
    # 1. Controllo per campi del filesystem
    if field in file_system_fields or field.startswith("st_"):
        classification_reason = "filesystem field"
        result = f"{BASE_URIS['technical_metadata_set']}FileSystemMetadata"
    
    # 2. Controllo per campi di sicurezza 
    elif field in security_fields:
        classification_reason = "security field"
        result = f"{BASE_URIS['technical_metadata_set']}SecurityMetadata"
    
    # 3. Controllo per campi specifici delle 
    elif field in image_specific_fields:
        classification_reason = "image-specific field"
        result = f"{BASE_URIS['technical_metadata_set']}ImageMetadata"
    
    # 4. Controllo per campi specifici dell'audio
    elif field in audio_specific_fields:
        classification_reason = "audio-specific field"
        result = f"{BASE_URIS['technical_metadata_set']}AudioMetadata"
    
    # 5. Controllo per campi specifici del video
    elif field in video_specific_fields:
        classification_reason = "video-specific field"
        result = f"{BASE_URIS['technical_metadata_set']}VideoMetadata"
    
    # 6. Controllo per campi email
    elif field in email_fields:
        classification_reason = "email field"
        result = f"{BASE_URIS['technical_metadata_set']}EmailMetadata"
    
    # 7. Controllo per campi eseguibili
    elif field in executable_fields:
        classification_reason = "executable field"
        result = f"{BASE_URIS['technical_metadata_set']}ExecutableMetadata"
    
    # 8. Controllo per campi archivi compressi
    elif field in archive_fields:
        classification_reason = "archive field"
        result = f"{BASE_URIS['technical_metadata_set']}CompressedFileMetadata"
    
    # 9. Controllo per campi documenti (dopo i controlli specifici)
    elif field in document_content_fields:
        classification_reason = "document content field"
        result = f"{BASE_URIS['technical_metadata_set']}DocumentContentMetadata"
    
    # 10. Fallback basato su mime type se fornito
    elif mime_type:
        mime_category = get_mime_type_category(mime_type)
        if mime_category == "image":
            classification_reason = f"mime type category: {mime_category}"
            result = f"{BASE_URIS['technical_metadata_set']}ImageMetadata"
        elif mime_category == "audio":
            classification_reason = f"mime type category: {mime_category}"
            result = f"{BASE_URIS['technical_metadata_set']}AudioMetadata"
        elif mime_category == "video":
            classification_reason = f"mime type category: {mime_category}"
            result = f"{BASE_URIS['technical_metadata_set']}VideoMetadata"
        else:
            classification_reason = f"mime type fallback (category: {mime_category})"
            result = f"{BASE_URIS['technical_metadata_set']}Other"
    
    # 11. Fallback finale per campi non classificati
    else:
        classification_reason = "unclassified field"
        result = f"{BASE_URIS['technical_metadata_set']}Other"

    if debug:
        print(f"[METADATASET] Field '{field}' > {result.split('/')[1]} ({classification_reason}) - relations_update_graph.py:518")
        if mime_type:
            print(f"[METADATASET]   MIME type: {mime_type} - relations_update_graph.py:520")
    
    return result



@dataclass
class DuplicateGroup:
    """Gruppo di istanziazioni con lo stesso hash"""
    hash_value: str
    instantiations: List[str]
    paths: List[str]
    count: int
    relationships_to_create: int = 0

    def __post_init__(self):
        self.relationships_to_create = self.count * (self.count - 1)


@dataclass
class CreationDateRecord:
    """Record con informazioni sulla data di creazione"""
    record_uri: str
    instantiation_uri: str
    metadata_value: str
    normalized_date: str
    date_uri: str
    file_path: str = ""


@dataclass
class RecordModificationDateRecord:
    """Record con informazioni sulla data di modifica da dcterms:modified"""
    record_uri: str
    instantiation_uri: str
    metadata_value: str
    normalized_date: str
    date_uri: str
    file_path: str = ""


@dataclass
class RecordSetModificationDateRecord:
    """RecordSet con informazioni sulla data di modifica da st_mtime"""
    recordset_uri: str
    instantiation_uri: str
    metadata_value: str
    normalized_date: str
    date_uri: str
    file_path: str = ""


@dataclass
class InstantiationModificationDateRecord:
    """Instantiation con informazioni sulla data di modifica da st_mtime"""
    instantiation_uri: str
    metadata_value: str
    normalized_date: str
    date_uri: str
    file_path: str = ""

@dataclass
class MimeTypeRecord:
    """Record per instantiation con MIME type da classificare"""
    instantiation_uri: str
    mime_type: str
    category: str
    file_path: str = ""

@dataclass
class TitleRecord:
    """Record/RecordSet con informazioni per la creazione del Title"""
    entity_uri: str
    entity_type: str  # "Record" o "RecordSet" 
    label_value: str
    title_uri: str

@dataclass
class TechnicalMetadataTypeSetRecord:
    """TechnicalMetadataTypeSet da creare"""
    set_uri: str
    set_label: str
    set_type: str  # Tipo del metadata set

@dataclass
@dataclass
class ProcessingResult:
    """Risultato complessivo delle operazioni"""
    # Hash duplicati
    total_duplicate_groups: int = 0
    total_instantiations_involved: int = 0
    total_hash_relationships_created: int = 0
    
    # Date creazione Record
    total_records_with_dates: int = 0
    total_date_entities_created: int = 0
    total_date_relationships_created: int = 0
    
    # Date creazione RecordSet
    total_recordsets_with_dates: int = 0
    total_recordset_date_relationships_created: int = 0
    
    # Date creazione Instantiation - AGGIUNTO!
    total_instantiations_with_dates: int = 0
    total_instantiation_date_relationships_created: int = 0
    
    # Date modifica Record
    total_records_with_modification_dates: int = 0
    total_record_modification_date_relationships_created: int = 0
    
    # Date modifica RecordSet
    total_recordsets_with_modification_dates: int = 0
    total_recordset_modification_date_relationships_created: int = 0
    
    # Date modifica Instantiation
    total_instantiations_with_modification_dates: int = 0
    total_instantiation_modification_date_relationships_created: int = 0

    # Title generation
    total_titles_created: int = 0
    total_title_relationships_created: int = 0

    # TechnicalMetadataTypeSet generation  
    total_technical_metadata_sets_created: int = 0

    # MIME Type classification
    total_mime_type_classifications: int = 0
    total_mime_type_relationships_created: int = 0

    # N-Quads export
    nquads_file: str = ""
    total_nquads_written: int = 0

    processing_time_seconds: float = 0
    
    # Liste di oggetti (inizializzate in __post_init__)
    errors: List[str] = None  # UNA SOLA VOLTA!
    duplicate_groups: List[DuplicateGroup] = None
    creation_date_records: List[CreationDateRecord] = None
    record_modification_date_records: List[RecordModificationDateRecord] = None
    recordset_modification_date_records: List[RecordSetModificationDateRecord] = None
    instantiation_modification_date_records: List[InstantiationModificationDateRecord] = None
    title_records: List[TitleRecord] = None
    technical_metadata_set_records: List[TechnicalMetadataTypeSetRecord] = None
    mime_type_records: List[MimeTypeRecord] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.duplicate_groups is None:
            self.duplicate_groups = []
        if self.creation_date_records is None:
            self.creation_date_records = []
        if self.record_modification_date_records is None:
            self.record_modification_date_records = []
        if self.recordset_modification_date_records is None:
            self.recordset_modification_date_records = []
        if self.instantiation_modification_date_records is None:
            self.instantiation_modification_date_records = []
        if self.title_records is None:
            self.title_records = []
        if self.mime_type_records is None:
            self.mime_type_records = []
        if self.technical_metadata_set_records is None:
            self.technical_metadata_set_records = []

class EnhancedRelationshipGenerator:
    """Generatore esteso per relazioni hash e date di creazione/modifica con supporto grafo e N-Quads automatico"""
    
    def __init__(self, endpoint_url: str = "http://localhost:10214/blazegraph/namespace/kb/sparql", 
                 target_graph: str = "http://ficlit.unibo.it/ArchivioEvangelisti/updated_relations",
                 export_nquads: bool = False,
                 always_save_nquads: bool = True):
        self.endpoint = endpoint_url
        self.target_graph = target_graph  # Grafo di destinazione per le triple
        self.export_nquads = export_nquads  # Flag per esportare in N-Quads invece di inserire
        self.always_save_nquads = always_save_nquads  # Flag per salvare sempre N-Quads
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'EnhancedRelationshipGenerator/1.3'
        })
        
        # Prefissi standard
        self.prefixes = """
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX bodi: <http://w3id.org/bodi#>
        PREFIX premis: <http://www.loc.gov/premis/rdf/v3/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dc: <http://purl.org/dc/terms/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        """
        
        self.logger = self._setup_logger()
        self.nquads_triples = []  # Buffer per triple N-Quads
        
    def _setup_logger(self):
        """Setup logger con formato dettagliato"""
        logging.basicConfig(
            level=logging.INFO,
            format='[%(levelname)s] %(asctime)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        return logging.getLogger('EnhancedRelationshipGenerator')
    
    def test_connection(self) -> bool:
        """Testa la connessione a Blazegraph (saltato se export_nquads=True)"""
        if self.export_nquads:
            self.logger.info("✅ Modalità N-Quads attiva - connessione Blazegraph non richiesta per inserimento")
            self.logger.info("⚠️ Connessione comunque necessaria per le query di ricerca dati")
        
        try:
            query = self.prefixes + "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                self.logger.info(f"✅ Connessione OK - Triple nel dataset: {count:,}")
                self.logger.info(f"🎯 Grafo di destinazione: <{self.target_graph}>")
                if self.always_save_nquads:
                    self.logger.info("💾 File N-Quads sarà creato automaticamente")
                return True
            else:
                self.logger.error(f"❌ Connessione fallita: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.logger.error(f"❌ Server Blazegraph non raggiungibile: {self.endpoint}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Errore test connessione: {e}")
            return False

    def convert_triple_to_nquads(self, triple: str) -> str:
        """Converte una tripla RDF in formato N-Quads con il grafo specificato"""
        # Rimuovi il punto finale se presente
        if triple.endswith(' .'):
            triple = triple[:-2]
        
        # Aggiungi il grafo e il punto finale
        return f"{triple} <{self.target_graph}> .\n"

    def save_nquads_to_file(self, filename: str = None) -> str:
        """Salva tutte le triple accumulate in un file N-Quads"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"relations_update_{timestamp}.nq"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for triple in self.nquads_triples:
                    f.write(triple)
            
            self.logger.info(f"💾 N-Quads salvati: {filename}")
            self.logger.info(f"📊 Triple scritte: {len(self.nquads_triples):,}")
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio N-Quads: {e}")
            return None

    def normalize_date(self, date_value: str) -> Optional[str]:
        """Normalizza diversi formati di data in formato ISO (YYYY-MM-DD)"""
        if not date_value or not isinstance(date_value, str):
            return None
            
        # Rimuovi spazi extra
        date_value = date_value.strip()
        
        # Controlla se è un timestamp Unix (anche in notazione scientifica)
        try:
            # Gestisce notazione scientifica come 1.75233369117012E9
            if 'E' in date_value.upper() or 'e' in date_value:
                timestamp = float(date_value)
            elif '.' in date_value and len(date_value.replace('.', '')) >= 10:
                # Timestamp Unix con decimali come 1752333691.17012
                timestamp = float(date_value)
            elif date_value.isdigit() and len(date_value) >= 10:
                # Timestamp Unix intero come 1752333691
                timestamp = float(date_value)
            else:
                timestamp = None
                
            if timestamp is not None:
                # Verifica che sia un timestamp Unix ragionevole
                # (tra 1970-01-01 e 2100-01-01, circa)
                if 0 <= timestamp <= 4102444800:  # 2100-01-01
                    try:
                        from datetime import datetime
                        dt = datetime.fromtimestamp(timestamp)
                        return dt.strftime("%Y-%m-%d")
                    except (ValueError, OSError, OverflowError):
                        pass
        except (ValueError, OverflowError):
            pass
        
        # ✅ GESTIONE FORMATO ISO 8601 COMPLETO
        try:
            # Python 3.7+ con fromisoformat (preferito se disponibile)
            try:
                from datetime import datetime
                # Prova prima con fromisoformat se supportato
                if hasattr(datetime, 'fromisoformat'):
                    # fromisoformat gestisce automaticamente molti formati ISO 8601
                    dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                    return dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass
                
            # Fallback: parsing manuale per formati ISO 8601 specifici
            iso_patterns = [
                # ISO 8601 completo con timezone UTC (Z)
                ("%Y-%m-%dT%H:%M:%SZ", "ISO 8601 UTC"),
                ("%Y-%m-%dT%H:%M:%S.%fZ", "ISO 8601 UTC con microsecondi"),
                
                # ISO 8601 completo con offset timezone (+/-HH:MM)
                ("%Y-%m-%dT%H:%M:%S%z", "ISO 8601 con timezone offset"),
                ("%Y-%m-%dT%H:%M:%S.%f%z", "ISO 8601 con microsecondi e timezone"),
                
                # ISO 8601 senza timezone
                ("%Y-%m-%dT%H:%M:%S", "ISO 8601 senza timezone"),
                ("%Y-%m-%dT%H:%M:%S.%f", "ISO 8601 con microsecondi senza timezone"),
            ]
            
            for pattern, description in iso_patterns:
                try:
                    # Aggiusta il formato timezone per Python < 3.7
                    test_value = date_value
                    if pattern.endswith('%z') and '+' in test_value and ':' in test_value[-6:]:
                        # Converte +05:00 in +0500 per Python < 3.7
                        test_value = test_value[:-3] + test_value[-2:]
                    elif pattern.endswith('%z') and '-' in test_value and ':' in test_value[-6:]:
                        # Converte -05:00 in -0500 per Python < 3.7
                        test_value = test_value[:-3] + test_value[-2:]
                    
                    dt = datetime.strptime(test_value, pattern)
                    self.logger.debug(f"✅ Parsed '{date_value}' using {description}")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"⚠️ Errore parsing ISO 8601 per '{date_value}': {e}")
        
        # Pattern comuni per date tradizionali (codice esistente)
        patterns = [
            # ISO format YYYY-MM-DD
            r'^(\d{4})-(\d{1,2})-(\d{1,2})$',
            # DD/MM/YYYY
            r'^(\d{1,2})/(\d{1,2})/(\d{4})$',
            # DD-MM-YYYY
            r'^(\d{1,2})-(\d{1,2})-(\d{4})$',
            # MM/DD/YYYY (formato US)
            r'^(\d{1,2})/(\d{1,2})/(\d{4})$',
            # YYYY/MM/DD
            r'^(\d{4})/(\d{1,2})/(\d{1,2})$',
            # Solo anno YYYY
            r'^(\d{4})$',
            # DD.MM.YYYY
            r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$'
        ]
        
        try:
            # Pattern ISO (già normalizzato)
            match = re.match(patterns[0], date_value)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            # Pattern DD/MM/YYYY
            match = re.match(patterns[1], date_value)
            if match:
                day, month, year = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            # Pattern DD-MM-YYYY
            match = re.match(patterns[2], date_value)
            if match:
                day, month, year = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            # Pattern YYYY/MM/DD
            match = re.match(patterns[4], date_value)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            # Solo anno
            match = re.match(patterns[5], date_value)
            if match:
                year = match.groups()[0]
                return f"{year}-01-01"  # Default al 1 gennaio
                
            # Pattern DD.MM.YYYY
            match = re.match(patterns[6], date_value)
            if match:
                day, month, year = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            # Tenta parsing con datetime per altri formati
            try:
                parsed_date = datetime.strptime(date_value, "%Y-%m-%d")
                return parsed_date.strftime("%Y-%m-%d")
            except:
                pass
                
            try:
                parsed_date = datetime.strptime(date_value, "%d/%m/%Y")
                return parsed_date.strftime("%Y-%m-%d")
            except:
                pass
                
            try:
                parsed_date = datetime.strptime(date_value, "%Y")
                return parsed_date.strftime("%Y-01-01")
            except:
                pass
            
            self.logger.warning(f"⚠️ Formato data non riconosciuto: '{date_value}'")
            return None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Errore normalizzazione data '{date_value}': {e}")
            return None

    def format_date_natural_language(self, normalized_date: str) -> str:
        """Converte data ISO in formato linguaggio naturale"""
        try:
            # Parse della data normalizzata ISO
            dt = datetime.strptime(normalized_date, "%Y-%m-%d")
            # Formato: "01 January 2025"
            return dt.strftime("%d %B %Y")
        except Exception:
            # Fallback: usa la data normalizzata così com'è
            return normalized_date

    def generate_date_uri(self, normalized_date: str) -> str:
        """Genera URI per entità Date COERENTE con evangelisti_metadata_extraction.py"""
        # Formato: http://ficlit.unibo.it/ArchivioEvangelisti/date_YYYYMMDD
        date_formatted = normalized_date.replace('-', '')  # 2025-01-01 → 20250101
        return f"{BASE_URIS['date']}{date_formatted}"

    def generate_technical_metadata_set_uri(self, set_type: str) -> str:
        """Genera URI per TechnicalMetadataTypeSet con base URI archivio"""
        return f"{BASE_URIS['technical_metadata_set']}{set_type}"

    def find_records_with_creation_dates(self) -> List[CreationDateRecord]:
        """Trova Record con metadati dcterms:created dalle loro istanziazioni"""
        self.logger.info("📅 RICERCA RECORD CON DATE DI CREAZIONE...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?record ?instantiation ?metadataValue ?filePath WHERE {
                ?record rico:hasOrHadInstantiation ?instantiation . 
                ?instantiation rdf:type rico:Instantiation .
                ?instantiation bodi:hasTechnicalMetadata ?metadata .
                ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                ?metadataType rdfs:label "dcterms:created" .
                ?metadata rdf:value ?metadataValue .
                
                OPTIONAL {
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?filePath .
                }

            
            FILTER(STRLEN(STR(?metadataValue)) > 0)
        }
        ORDER BY ?instantiation
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            creation_date_records = []
            
            for binding in bindings:
                record_uri = binding["record"]["value"]
                instantiation_uri = binding["instantiation"]["value"]
                metadata_value = binding["metadataValue"]["value"]
                file_path = binding.get("filePath", {}).get("value", "Path non disponibile")
                
                # Normalizza la data
                normalized_date = self.normalize_date(metadata_value)
                
                if normalized_date:
                    date_uri = self.generate_date_uri(normalized_date)
                    
                    creation_date_record = CreationDateRecord(
                        record_uri=record_uri,
                        instantiation_uri=instantiation_uri,
                        metadata_value=metadata_value,
                        normalized_date=normalized_date,
                        date_uri=date_uri,
                        file_path=file_path
                    )
                    
                    creation_date_records.append(creation_date_record)
                else:
                    self.logger.warning(f"⚠️ Data non normalizzabile: {metadata_value} per {record_uri}")
            
            self.logger.info(f"✅ Query completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI DATE CREAZIONE:")
            self.logger.info(f"   📅 Record con date valide: {len(creation_date_records)}")
            self.logger.info(f"   📁 Istanziazioni coinvolte: {len(set(r.instantiation_uri for r in creation_date_records))}")
            
            if creation_date_records:
                unique_dates = len(set(r.normalized_date for r in creation_date_records))
                self.logger.info(f"   🗓️ Date uniche: {unique_dates}")
                self.logger.info(f"   🔗 Relazioni da creare: {len(creation_date_records)}")
                self.logger.info(f"   📝 Triple per ogni entità Date: 3 (tipo, normalizzata, espressa)")
                
                # Mostra esempi con valore originale e normalizzato
                self.logger.info("   📝 Esempi di date trovate:")
                for i, record in enumerate(creation_date_records[:3]):
                    record_short = record.record_uri.split('/')[-1]
                    self.logger.info(f"      {i+1}. {record_short}: '{record.metadata_value}' → {record.normalized_date}")
                if len(creation_date_records) > 3:
                    self.logger.info(f"      ... e altre {len(creation_date_records) - 3} date")
            
            return creation_date_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca date creazione: {e}")
            return []
    
    def find_records_with_modification_dates(self) -> List[RecordModificationDateRecord]:
        """Trova Record con metadati dcterms:modified dalle loro istanziazioni"""
        self.logger.info("📅 RICERCA RECORD CON DATE DI MODIFICA (dcterms:modified)...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?record ?instantiation ?metadataValue ?filePath WHERE {
                ?record rico:hasOrHadInstantiation ?instantiation . 
                ?instantiation rdf:type rico:Instantiation .
                ?instantiation bodi:hasTechnicalMetadata ?metadata .
                ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                ?metadataType rdfs:label "dcterms:modified" .
                ?metadata rdf:value ?metadataValue .
                
                OPTIONAL {
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?filePath .
                }
            
            FILTER(STRLEN(STR(?metadataValue)) > 0)
        }
        ORDER BY ?instantiation
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query Record modifica fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            record_modification_date_records = []
            
            for binding in bindings:
                record_uri = binding["record"]["value"]
                instantiation_uri = binding["instantiation"]["value"]
                metadata_value = binding["metadataValue"]["value"]
                file_path = binding.get("filePath", {}).get("value", "Path non disponibile")
                
                # Normalizza la data
                normalized_date = self.normalize_date(metadata_value)
                
                if normalized_date:
                    date_uri = self.generate_date_uri(normalized_date)
                    
                    record_modification_date_record = RecordModificationDateRecord(
                        record_uri=record_uri,
                        instantiation_uri=instantiation_uri,
                        metadata_value=metadata_value,
                        normalized_date=normalized_date,
                        date_uri=date_uri,
                        file_path=file_path
                    )
                    
                    record_modification_date_records.append(record_modification_date_record)
                else:
                    self.logger.warning(f"⚠️ Data dcterms:modified non normalizzabile: {metadata_value} per {record_uri}")
            
            self.logger.info(f"✅ Query Record modifica completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI DATE MODIFICA RECORD:")
            self.logger.info(f"   📅 Record con date dcterms:modified valide: {len(record_modification_date_records)}")
            self.logger.info(f"   📁 Istanziazioni coinvolte: {len(set(r.instantiation_uri for r in record_modification_date_records))}")
            
            if record_modification_date_records:
                unique_dates = len(set(r.normalized_date for r in record_modification_date_records))
                self.logger.info(f"   🗓️ Date uniche: {unique_dates}")
                self.logger.info(f"   🔗 Relazioni bidirezionali da creare: {len(record_modification_date_records)} coppie")
                
                # Mostra esempi con valore originale e normalizzado
                self.logger.info("   📝 Esempi di date dcterms:modified trovate:")
                for i, record in enumerate(record_modification_date_records[:3]):
                    record_short = record.record_uri.split('/')[-1]
                    self.logger.info(f"      {i+1}. {record_short}: '{record.metadata_value}' → {record.normalized_date}")
                if len(record_modification_date_records) > 3:
                    self.logger.info(f"      ... e altri {len(record_modification_date_records) - 3} Record")
            
            return record_modification_date_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca date modifica Record: {e}")
            return []
        
    def find_records_with_filesystem_modification_dates(self) -> List[RecordModificationDateRecord]:
        """Trova Record che NON hanno dcterms:modified ma le cui istanziazioni hanno st_mtime"""
        self.logger.info("📅 RICERCA RECORD CON DATE FILESYSTEM (FALLBACK st_mtime)...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?record ?instantiation ?metadataValue ?filePath WHERE {
                ?record rico:hasOrHadInstantiation ?instantiation . 
                ?instantiation rdf:type rico:Instantiation .
                ?instantiation bodi:hasTechnicalMetadata ?metadata .
                ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                ?metadataType rdfs:label "st_mtime" .
                ?metadata rdf:value ?metadataValue .
                
                OPTIONAL {
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?filePath .
                }
                
                # FILTRO: Il Record NON deve avere già dcterms:modified
                FILTER NOT EXISTS {
                        ?record rico:hasOrHadInstantiation ?anyInst .
                        ?anyInst bodi:hasTechnicalMetadata ?anyMeta .
                        ?anyMeta bodi:hasTechnicalMetadataType ?anyType .
                        ?anyType rdfs:label "dcterms:modified" .
                
            }
            
            FILTER(STRLEN(STR(?metadataValue)) > 0)
        }
        ORDER BY ?instantiation
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query Record filesystem fallback fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            record_filesystem_modification_date_records = []
            
            for binding in bindings:
                record_uri = binding["record"]["value"]
                instantiation_uri = binding["instantiation"]["value"]
                metadata_value = binding["metadataValue"]["value"]
                file_path = binding.get("filePath", {}).get("value", "Path non disponibile")
                
                # Normalizza la data
                normalized_date = self.normalize_date(metadata_value)
                
                if normalized_date:
                    date_uri = self.generate_date_uri(normalized_date)
                    
                    record_filesystem_modification_date_record = RecordModificationDateRecord(
                        record_uri=record_uri,
                        instantiation_uri=instantiation_uri,
                        metadata_value=metadata_value,
                        normalized_date=normalized_date,
                        date_uri=date_uri,
                        file_path=file_path
                    )
                    
                    record_filesystem_modification_date_records.append(record_filesystem_modification_date_record)
                else:
                    self.logger.warning(f"⚠️ Data st_mtime non normalizzabile: {metadata_value} per {record_uri}")
            
            self.logger.info(f"✅ Query fallback filesystem completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI DATE FILESYSTEM FALLBACK:")
            self.logger.info(f"   📅 Record senza dcterms:modified ma con st_mtime: {len(record_filesystem_modification_date_records)}")
            self.logger.info(f"   📁 Istanziazioni coinvolte: {len(set(r.instantiation_uri for r in record_filesystem_modification_date_records))}")
            
            if record_filesystem_modification_date_records:
                unique_dates = len(set(r.normalized_date for r in record_filesystem_modification_date_records))
                self.logger.info(f"   🗓️ Date uniche: {unique_dates}")
                self.logger.info(f"   🔗 Relazioni bidirezionali da creare: {len(record_filesystem_modification_date_records)} coppie")
                
                # Mostra esempi con valore originale e normalizzato
                self.logger.info("   📝 Esempi di date st_mtime fallback trovate:")
                for i, record in enumerate(record_filesystem_modification_date_records[:3]):
                    record_short = record.record_uri.split('/')[-1]
                    self.logger.info(f"      {i+1}. {record_short}: '{record.metadata_value}' → {record.normalized_date}")
                if len(record_filesystem_modification_date_records) > 3:
                    self.logger.info(f"      ... e altri {len(record_filesystem_modification_date_records) - 3} Record")
            
            return record_filesystem_modification_date_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca date filesystem fallback: {e}")
            return []

    def find_recordsets_with_modification_dates(self) -> List[RecordSetModificationDateRecord]:
        """Trova RecordSet con metadati st_mtime dalle loro istanziazioni"""
        self.logger.info("📅 RICERCA RECORDSET CON DATE DI MODIFICA (st_mtime)...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?recordset ?instantiation ?metadataValue ?filePath WHERE {
                ?recordset rico:hasOrHadInstantiation ?instantiation . 
                ?recordset rdf:type rico:RecordSet .
                ?instantiation rdf:type rico:Instantiation .
                ?instantiation bodi:hasTechnicalMetadata ?metadata .
                ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                ?metadataType rdfs:label "st_mtime" .
                ?metadata rdf:value ?metadataValue .
                
                OPTIONAL {
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?filePath .
                }
            
            FILTER(STRLEN(STR(?metadataValue)) > 0)
        }
        ORDER BY ?instantiation
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query RecordSet modifica fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            recordset_modification_date_records = []
            
            for binding in bindings:
                recordset_uri = binding["recordset"]["value"]
                instantiation_uri = binding["instantiation"]["value"]
                metadata_value = binding["metadataValue"]["value"]
                file_path = binding.get("filePath", {}).get("value", "Path non disponibile")
                
                # Normalizza la data
                normalized_date = self.normalize_date(metadata_value)
                
                if normalized_date:
                    date_uri = self.generate_date_uri(normalized_date)
                    
                    recordset_modification_date_record = RecordSetModificationDateRecord(
                        recordset_uri=recordset_uri,
                        instantiation_uri=instantiation_uri,
                        metadata_value=metadata_value,
                        normalized_date=normalized_date,
                        date_uri=date_uri,
                        file_path=file_path
                    )
                    
                    recordset_modification_date_records.append(recordset_modification_date_record)
                else:
                    self.logger.warning(f"⚠️ Data st_mtime non normalizzabile: {metadata_value} per {recordset_uri}")
            
            self.logger.info(f"✅ Query RecordSet modifica completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI DATE MODIFICA RECORDSET:")
            self.logger.info(f"   📅 RecordSet con date st_mtime valide: {len(recordset_modification_date_records)}")
            self.logger.info(f"   📁 Istanziazioni coinvolte: {len(set(r.instantiation_uri for r in recordset_modification_date_records))}")
            
            if recordset_modification_date_records:
                unique_dates = len(set(r.normalized_date for r in recordset_modification_date_records))
                self.logger.info(f"   🗓️ Date uniche: {unique_dates}")
                self.logger.info(f"   🔗 Relazioni bidirezionali da creare: {len(recordset_modification_date_records)} coppie")
                
                # Mostra esempi con valore originale e normalizzato
                self.logger.info("   📝 Esempi di date st_mtime trovate:")
                for i, record in enumerate(recordset_modification_date_records[:3]):
                    recordset_short = record.recordset_uri.split('/')[-1]
                    self.logger.info(f"      {i+1}. {recordset_short}: '{record.metadata_value}' → {record.normalized_date}")
                if len(recordset_modification_date_records) > 3:
                    self.logger.info(f"      ... e altri {len(recordset_modification_date_records) - 3} RecordSet")
            
            return recordset_modification_date_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca date modifica RecordSet: {e}")
            return []

    def find_instantiations_with_modification_dates(self) -> List[InstantiationModificationDateRecord]:
        """Trova Instantiation con metadati st_mtime propri"""
        self.logger.info("📅 RICERCA INSTANTIATION CON DATE DI MODIFICA (st_mtime)...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?instantiation ?metadataValue ?filePath WHERE {
                ?instantiation rdf:type rico:Instantiation .
                ?instantiation bodi:hasTechnicalMetadata ?metadata .
                ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                ?metadataType rdfs:label "st_mtime" .
                ?metadata rdf:value ?metadataValue .
                
                OPTIONAL {
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?filePath .
                }
            
            FILTER(STRLEN(STR(?metadataValue)) > 0)
        }
        ORDER BY ?instantiation
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query Instantiation modifica fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            instantiation_modification_date_records = []
            
            for binding in bindings:
                instantiation_uri = binding["instantiation"]["value"]
                metadata_value = binding["metadataValue"]["value"]
                file_path = binding.get("filePath", {}).get("value", "Path non disponibile")
                
                # Normalizza la data
                normalized_date = self.normalize_date(metadata_value)
                
                if normalized_date:
                    date_uri = self.generate_date_uri(normalized_date)
                    
                    instantiation_modification_date_record = InstantiationModificationDateRecord(
                        instantiation_uri=instantiation_uri,
                        metadata_value=metadata_value,
                        normalized_date=normalized_date,
                        date_uri=date_uri,
                        file_path=file_path
                    )
                    
                    instantiation_modification_date_records.append(instantiation_modification_date_record)
                else:
                    self.logger.warning(f"⚠️ Data st_mtime non normalizzabile: {metadata_value} per {instantiation_uri}")
            
            self.logger.info(f"✅ Query Instantiation modifica completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI DATE MODIFICA INSTANTIATION:")
            self.logger.info(f"   📅 Instantiation con date st_mtime valide: {len(instantiation_modification_date_records)}")
            
            if instantiation_modification_date_records:
                unique_dates = len(set(r.normalized_date for r in instantiation_modification_date_records))
                self.logger.info(f"   🗓️ Date uniche: {unique_dates}")
                self.logger.info(f"   🔗 Relazioni bidirezionali da creare: {len(instantiation_modification_date_records)} coppie")
                
                # Mostra esempi con valore originale e normalizzado
                self.logger.info("   📝 Esempi di date st_mtime trovate:")
                for i, record in enumerate(instantiation_modification_date_records[:3]):
                    instantiation_short = record.instantiation_uri.split('/')[-1]
                    self.logger.info(f"      {i+1}. {instantiation_short}: '{record.metadata_value}' → {record.normalized_date}")
                if len(instantiation_modification_date_records) > 3:
                    self.logger.info(f"      ... e altre {len(instantiation_modification_date_records) - 3} Instantiation")
            
            return instantiation_modification_date_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca date modifica Instantiation: {e}")
            return []

    def get_existing_creation_date_relationships(self) -> Set[str]:
        """Ottiene tutte le relazioni rico:hasCreationDate già esistenti"""
        if self.export_nquads:
            # In modalità N-Quads non controlliamo relazioni esistenti
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI DATE ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?record WHERE {
            ?record rico:hasCreationDate ?date .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query relazioni date esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_records = set()
            for binding in bindings:
                record_uri = binding["record"]["value"]
                existing_records.add(record_uri)
            
            self.logger.info(f"✅ Record con date già esistenti: {len(existing_records)}")
            return existing_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo relazioni date: {e}")
            return set()

    def get_existing_modification_date_relationships(self) -> Set[str]:
        """Ottiene tutte le relazioni rico:hasModificationDate già esistenti per Record"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI DATE MODIFICA RECORD ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?record WHERE {
            ?record rico:hasModificationDate ?date .
            ?record rdf:type rico:Record .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query relazioni modifica Record esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_records = set()
            for binding in bindings:
                record_uri = binding["record"]["value"]
                existing_records.add(record_uri)
            
            self.logger.info(f"✅ Record con date modifica già esistenti: {len(existing_records)}")
            return existing_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo relazioni modifica Record: {e}")
            return set()

    def get_existing_recordset_modification_date_relationships(self) -> Set[str]:
        """Ottiene tutte le relazioni rico:hasModificationDate per RecordSet già esistenti"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI DATE MODIFICA RECORDSET ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?recordset WHERE {
            ?recordset rico:hasModificationDate ?date .
            ?recordset rdf:type rico:RecordSet .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query relazioni modifica RecordSet esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_recordsets = set()
            for binding in bindings:
                recordset_uri = binding["recordset"]["value"]
                existing_recordsets.add(recordset_uri)
            
            self.logger.info(f"✅ RecordSet con date modifica già esistenti: {len(existing_recordsets)}")
            return existing_recordsets
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo relazioni modifica RecordSet: {e}")
            return set()

    def get_existing_instantiation_modification_date_relationships(self) -> Set[str]:
        """Ottiene tutte le relazioni rico:hasModificationDate per Instantiation già esistenti"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI DATE MODIFICA INSTANTIATION ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?instantiation WHERE {
            ?instantiation rico:hasModificationDate ?date .
            ?instantiation rdf:type rico:Instantiation .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query relazioni modifica Instantiation esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_instantiations = set()
            for binding in bindings:
                instantiation_uri = binding["instantiation"]["value"]
                existing_instantiations.add(instantiation_uri)
            
            self.logger.info(f"✅ Instantiation con date modifica già esistenti: {len(existing_instantiations)}")
            return existing_instantiations
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo relazioni modifica Instantiation: {e}")
            return set()
    
    def find_records_and_recordsets_for_titles(self) -> List[TitleRecord]:
        """Trova tutti i Record e RecordSet per creare i loro Title"""
        self.logger.info("📝 RICERCA RECORD E RECORDSET PER TITLES...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?entity ?entityType ?label WHERE {
                {
                    ?entity rdf:type rico:Record .
                    BIND("Record" AS ?entityType)
                } UNION {
                    ?entity rdf:type rico:RecordSet .
                    BIND("RecordSet" AS ?entityType)  
                }
                ?entity rdfs:label ?label .
        }
        ORDER BY ?entity
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query Title fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            title_records = []
            
            for binding in bindings:
                entity_uri = binding["entity"]["value"]
                entity_type = binding["entityType"]["value"]
                label_value = binding["label"]["value"]
                
                # Estrai ID dall'URI per creare title URI
                entity_id = entity_uri.split('/')[-1]
                title_uri = f"http://ficlit.unibo.it/ArchivioEvangelisti/{entity_id}_title"
                
                title_record = TitleRecord(
                    entity_uri=entity_uri,
                    entity_type=entity_type,
                    label_value=label_value,
                    title_uri=title_uri
                )
                
                title_records.append(title_record)
            
            self.logger.info(f"✅ Query Title completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI TITLE:")
            self.logger.info(f"   📝 Entità trovate: {len(title_records)}")
            
            record_count = len([t for t in title_records if t.entity_type == "Record"])
            recordset_count = len([t for t in title_records if t.entity_type == "RecordSet"])
            
            self.logger.info(f"   📄 Record: {record_count}")
            self.logger.info(f"   📁 RecordSet: {recordset_count}")
            self.logger.info(f"   🔗 Title da creare: {len(title_records)}")
            
            return title_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca Title: {e}")
            return []

    def generate_title_triples(self, title_records: List[TitleRecord]) -> List[str]:
        """Genera triple RDF per i Title (solo se non esistenti)"""
        self.logger.info("📝 GENERAZIONE TRIPLE TITLE...")
        
        existing_entities = self.get_existing_title_relationships()
        
        triples = []
        titles_created = 0
        existing_skipped = 0
        
        for record in title_records:
            # Salta se l'entità ha già un Title
            if record.entity_uri in existing_entities:
                existing_skipped += 1
                continue
                
            # Triple per l'entità Title
            triples.append(f"<{record.title_uri}> rdf:type rico:Title .")
            triples.append(f"<{record.title_uri}> rdfs:label \"{record.label_value}\" .")
            
            # Triple per relazione bidirezionale Entity ↔ Title
            triples.append(f"<{record.entity_uri}> rico:hasOrHadTitle <{record.title_uri}> .")
            triples.append(f"<{record.title_uri}> rico:isTitleOf <{record.entity_uri}> .")
            titles_created += 1
        
        self.logger.info(f"✅ Generate {len(triples)} triple per Title")
        self.logger.info(f"   📊 Nuovi Title creati: {titles_created}")
        if existing_skipped > 0:
            self.logger.info(f"   ⚡ Entità con Title già esistenti saltate: {existing_skipped}")
        
        return triples

    
    def get_existing_title_relationships(self) -> Set[str]:
        """Ottiene tutte le entità che hanno già rico:hasOrHadTitle"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO TITLE ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?entity WHERE {
            ?entity rico:hasOrHadTitle ?title .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query Title esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_entities = set()
            for binding in bindings:
                entity_uri = binding["entity"]["value"]
                existing_entities.add(entity_uri)
            
            self.logger.info(f"✅ Entità con Title già esistenti: {len(existing_entities)}")
            return existing_entities
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo Title esistenti: {e}")
            return set()

    def create_technical_metadata_sets(self) -> List[TechnicalMetadataTypeSetRecord]:
        """Crea i TechnicalMetadataTypeSet"""
        self.logger.info("🗂️ CREAZIONE TECHNICAL METADATA SETS...")
        
        metadata_sets = {
            "FileSystemMetadata": "File system metadata",
            "DocumentContentMetadata": "Document content metadata", 
            "ImageMetadata": "Image metadata",
            "AudioMetadata": "Audio metadata",
            "VideoMetadata": "Video metadata",
            "EmailMetadata": "Email metadata",
            "ExecutableMetadata": "Executable metadata",
            "CompressedFileMetadata": "Compressed file metadata",
            "Other": "Other metadata",
        }
        
        metadata_set_records = []
        
        for set_type, set_label in metadata_sets.items():
            set_uri = f"{BASE_URIS['technical_metadata_set']}{set_type}"
            
            metadata_set_record = TechnicalMetadataTypeSetRecord(
                set_uri=set_uri,
                set_label=set_label,
                set_type=set_type
            )
            
            metadata_set_records.append(metadata_set_record)
        
        self.logger.info(f"✅ Definiti {len(metadata_set_records)} TechnicalMetadataTypeSet")
        
        return metadata_set_records

    def generate_technical_metadata_set_triples(self, metadata_set_records: List[TechnicalMetadataTypeSetRecord]) -> List[str]:
        """Genera triple RDF per i TechnicalMetadataTypeSet"""
        self.logger.info("🗂️ GENERAZIONE TRIPLE TECHNICAL METADATA SETS...")
        
        triples = []
        
        for record in metadata_set_records:
            # Triple per il TechnicalMetadataTypeSet
            triples.append(f"<{record.set_uri}> rdf:type bodi:TechnicalMetadataTypeSet .")
            triples.append(f"<{record.set_uri}> rdfs:label \"{record.set_label}\" .")
        
        self.logger.info(f"✅ Generate {len(triples)} triple per TechnicalMetadataTypeSet")
        self.logger.info(f"   📊 TechnicalMetadataTypeSet creati: {len(metadata_set_records)}")
        
        return triples
    
    def find_duplicate_hashes(self) -> List[DuplicateGroup]:
        """Trova tutti i gruppi di istanziazioni con hash identici"""
        self.logger.info("🔍 RICERCA HASH DUPLICATI...")
        
        query = self.prefixes + """
        SELECT ?hash_value 
            (GROUP_CONCAT(DISTINCT ?inst; separator="|") AS ?instantiations)
            (GROUP_CONCAT(DISTINCT ?path; separator="|") AS ?paths)
            (COUNT(DISTINCT ?inst) AS ?duplicate_count) 
        WHERE {
            GRAPH ?g {
                ?inst rdf:type rico:Instantiation .
                ?inst bodi:hasHashCode ?fixity .
                ?fixity rdf:type premis:Fixity .
                ?fixity rdf:value ?hash_value .
                
                OPTIONAL {
                    ?inst prov:atLocation ?location .
                    ?location rdfs:label ?path .
                }
            }
            
            FILTER(REGEX(STR(?hash_value), "^[a-fA-F0-9]{64}$"))
        }
        GROUP BY ?hash_value
        HAVING (COUNT(DISTINCT ?inst) > 1)
        ORDER BY DESC(?duplicate_count)
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            duplicate_groups = []
            total_duplicates = 0
            
            for binding in bindings:
                hash_value = binding["hash_value"]["value"]
                instantiations_str = binding["instantiations"]["value"]
                paths_str = binding.get("paths", {}).get("value", "")
                count = int(binding["duplicate_count"]["value"])
                
                instantiations = [inst.strip() for inst in instantiations_str.split("|") if inst.strip()]
                paths = [path.strip() for path in paths_str.split("|") if path.strip()] if paths_str else []
                
                while len(paths) < len(instantiations):
                    paths.append("Path non disponibile")
                
                duplicate_group = DuplicateGroup(
                    hash_value=hash_value,
                    instantiations=instantiations,
                    paths=paths[:len(instantiations)],
                    count=count
                )
                
                duplicate_groups.append(duplicate_group)
                total_duplicates += count
            
            self.logger.info(f"✅ Query completata in {query_time:.2f}s")
            self.logger.info(f"📊 RISULTATI DUPLICATI:")
            self.logger.info(f"   🔄 Gruppi hash duplicati: {len(duplicate_groups)}")
            self.logger.info(f"   📁 Istanziazioni coinvolte: {total_duplicates:,}")
            
            if duplicate_groups:
                max_group = max(duplicate_groups, key=lambda g: g.count)
                total_relationships = sum(g.relationships_to_create for g in duplicate_groups)
                self.logger.info(f"   📁 Gruppo più grande: {max_group.count} file identici")
                self.logger.info(f"   🔗 Relazioni da creare: {total_relationships:,}")
            
            return duplicate_groups
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca duplicati: {e}")
            return []

    def get_existing_hash_relationships(self) -> Set[Tuple[str, str]]:
        """Ottiene tutte le relazioni bodi:hasSameHashCodeAs già esistenti"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI HASH ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?inst1 ?inst2 WHERE {
            ?inst1 bodi:hasSameHashCodeAs ?inst2 .
        }
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query relazioni hash esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_relationships = set()
            for binding in bindings:
                inst1 = binding["inst1"]["value"]
                inst2 = binding["inst2"]["value"]
                existing_relationships.add((inst1, inst2))
            
            self.logger.info(f"✅ Controllo completato in {query_time:.2f}s")
            self.logger.info(f"📊 Relazioni hash già esistenti: {len(existing_relationships):,}")
            
            return existing_relationships
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo relazioni hash esistenti: {e}")
            return set()

    def create_metadata_type_to_set_relationships(self) -> List[str]:
        """Crea relazioni tra TechnicalMetadataType e TechnicalMetadataTypeSet (solo se non esistenti)"""
        self.logger.info("🔗 CREAZIONE RELAZIONI METADATATYPE → METADATASET...")
        
        existing_types = self.get_existing_metadata_type_set_relationships()
        
        # Query per ottenere tutti i TechnicalMetadataType
        query = self.prefixes + """
        SELECT DISTINCT ?metadataType ?label WHERE {
                ?metadataType rdf:type bodi:TechnicalMetadataType .
                ?metadataType rdfs:label ?label .
        }
        ORDER BY ?metadataType
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query MetadataType fallita: HTTP {response.status_code}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            triples = []
            processed_types = 0
            existing_skipped = 0
            
            for binding in bindings:
                metadata_type_uri = binding["metadataType"]["value"]
                label = binding["label"]["value"]
                
                # Salta se il TechnicalMetadataType ha già relazioni con un Set
                if metadata_type_uri in existing_types:
                    existing_skipped += 1
                    continue
                
                # Determina il metadata set appropriato
                metadata_set_uri = get_metadata_set_for_field(label)
                
                # Crea relazioni bidirezionali
                triples.append(f"<{metadata_type_uri}> rico:isOrWasPartOf <{metadata_set_uri}> .")
                triples.append(f"<{metadata_set_uri}> rico:hasOrHadPart <{metadata_type_uri}> .")
                
                processed_types += 1
            
            self.logger.info(f"✅ Create relazioni per {processed_types} TechnicalMetadataType")
            self.logger.info(f"   🔗 Triple generate: {len(triples)}")
            if existing_skipped > 0:
                self.logger.info(f"   ⚡ TechnicalMetadataType già collegati saltati: {existing_skipped}")
            
            return triples
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante creazione relazioni MetadataType-Set: {e}")
            return []


    def get_existing_metadata_type_set_relationships(self) -> Set[str]:
        """Ottiene tutti i TechnicalMetadataType che hanno già relazioni con TechnicalMetadataTypeSet"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI METADATATYPE-SET ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?metadataType WHERE {
            ?metadataType rico:isOrWasPartOf ?metadataTypeSet .
            ?metadataType rdf:type bodi:TechnicalMetadataType .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query relazioni MetadataType-Set esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_types = set()
            for binding in bindings:
                metadata_type_uri = binding["metadataType"]["value"]
                existing_types.add(metadata_type_uri)
            
            self.logger.info(f"✅ TechnicalMetadataType con relazioni già esistenti: {len(existing_types)}")
            return existing_types
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo relazioni MetadataType-Set: {e}")
            return set()

    def generate_hash_relationship_triples(self, duplicate_groups: List[DuplicateGroup]) -> List[str]:
        """Genera le triple RDF per le relazioni bodi:hasSameHashCodeAs (solo se non esistenti)"""
        self.logger.info("🔗 GENERAZIONE RELAZIONI HASH BIDIREZIONALI...")
        
        existing_relationships = self.get_existing_hash_relationships()
        
        triples = []
        total_relationships = 0
        self_relations_prevented = 0
        duplicate_relations_skipped = 0
        
        for group in duplicate_groups:
            if len(group.instantiations) < 2:
                continue
                
            for i, inst1 in enumerate(group.instantiations):
                for j, inst2 in enumerate(group.instantiations):
                    if i != j and inst1 != inst2:
                        if (inst1, inst2) not in existing_relationships:
                            triple = f"<{inst1}> bodi:hasSameHashCodeAs <{inst2}> ."
                            triples.append(triple)
                            total_relationships += 1
                        else:
                            duplicate_relations_skipped += 1
                    elif inst1 == inst2:
                        self_relations_prevented += 1
        
        self.logger.info(f"✅ Generate {total_relationships:,} nuove relazioni hash bidirezionali")
        self.logger.info(f"   📊 Per {len(duplicate_groups)} gruppi di duplicati")
        if self_relations_prevented > 0:
            self.logger.info(f"   🛡️ Auto-relazioni prevenute: {self_relations_prevented}")
        if duplicate_relations_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {duplicate_relations_skipped:,}")
        
        return triples

    def generate_creation_date_triples(self, creation_date_records: List[CreationDateRecord]) -> List[str]:
        """Genera triple RDF per date di creazione e relazioni seguendo le specifiche RiC-O"""
        self.logger.info("📅 GENERAZIONE TRIPLE DATE CREAZIONE...")
        
        existing_records = self.get_existing_creation_date_relationships()
        
        triples = []
        date_entities_created = {}  # Traccia date create
        relationships_created = 0
        existing_relationships_skipped = 0
        
        for record in creation_date_records:
            # Salta se relazione già esiste
            if record.record_uri in existing_records:
                existing_relationships_skipped += 1
                continue
            
            # Crea entità Date (solo una volta per data unica)
            if record.date_uri not in date_entities_created:
                # Triple per l'entità Date secondo RiC-O
                triples.append(f"<{record.date_uri}> rdf:type rico:Date .")
                
                # AGGIUNTO: rico:type per indicare la fonte
                triples.append(f'<{record.date_uri}> rico:type "Derived from embedded metadata" .')
                
                # rico:normalizedDateValue per il valore standardizzato ISO 8601 (senza tipo XSD)
                triples.append(f"<{record.date_uri}> rico:normalizedDateValue \"{record.normalized_date}\" .")
                
                # rico:expressedDate in linguaggio naturale (formato: "01 January 2025")
                natural_date = self.format_date_natural_language(record.normalized_date)
                triples.append(f"<{record.date_uri}> rico:expressedDate \"{natural_date}\" .")
                
                date_entities_created[record.date_uri] = {
                    'normalized': record.normalized_date,
                    'natural': natural_date
                }
            
            # Triple per relazione bidirezionale Record ↔ Date
            triples.append(f"<{record.record_uri}> rico:hasCreationDate <{record.date_uri}> .")
            triples.append(f"<{record.date_uri}> rico:isCreationDateOf <{record.record_uri}> .")
            relationships_created += 2  # Relazione bidirezionale
        
        self.logger.info(f"✅ Generate {len(triples)} triple per date di creazione (RiC-O compliant)")
        self.logger.info(f"   📊 Entità Date uniche create: {len(date_entities_created)}")
        self.logger.info(f"   🔗 Relazioni bidirezionali create: {relationships_created // 2} coppie")
        self.logger.info(f"   ↔️ Record→Date (hasCreationDate) + Date→Record (isCreationDateOf)")
        self.logger.info(f"   🏷️ Ogni entità Date ha rico:type 'Derived from embedded metadata'")
        self.logger.info(f"   📝 Ogni entità Date ha 4 proprietà: rdf:type, rico:type, normalizedDateValue, expressedDate")
        if existing_relationships_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {existing_relationships_skipped}")
        
        return triples

    def generate_record_modification_date_triples(self, record_modification_date_records: List[RecordModificationDateRecord]) -> List[str]:
        """Genera triple RDF per date di modifica Record e relazioni bidirezionali seguendo RiC-O"""
        self.logger.info("📅 GENERAZIONE TRIPLE DATE MODIFICA RECORD...")
        
        existing_records = self.get_existing_modification_date_relationships()
        
        triples = []
        date_entities_created = {}  # Traccia date create
        relationships_created = 0
        existing_relationships_skipped = 0
        
        for record in record_modification_date_records:
            # Salta se relazione già esiste
            if record.record_uri in existing_records:
                existing_relationships_skipped += 1
                continue
            
            # Crea entità Date (solo una volta per data unica)
            if record.date_uri not in date_entities_created:
                # Triple per l'entità Date secondo RiC-O
                triples.append(f"<{record.date_uri}> rdf:type rico:Date .")
                
                # AGGIUNTO: rico:type per indicare la fonte
                triples.append(f'<{record.date_uri}> rico:type "Derived from embedded metadata" .')
                
                # rico:normalizedDateValue per il valore standardizzato ISO 8601 (senza tipo XSD)
                triples.append(f"<{record.date_uri}> rico:normalizedDateValue \"{record.normalized_date}\" .")
                
                # rico:expressedDate in linguaggio naturale (formato: "01 January 2025")
                natural_date = self.format_date_natural_language(record.normalized_date)
                triples.append(f"<{record.date_uri}> rico:expressedDate \"{natural_date}\" .")
                
                date_entities_created[record.date_uri] = {
                    'normalized': record.normalized_date,
                    'natural': natural_date
                }
            
            # Triple per relazione bidirezionale Record ↔ Date
            triples.append(f"<{record.record_uri}> rico:hasModificationDate <{record.date_uri}> .")
            triples.append(f"<{record.date_uri}> rico:isModificationDateOf <{record.record_uri}> .")
            relationships_created += 2  # Relazione bidirezionale
        
        self.logger.info(f"✅ Generate {len(triples)} triple per date modifica Record (RiC-O compliant)")
        self.logger.info(f"   📊 Entità Date uniche create: {len(date_entities_created)}")
        self.logger.info(f"   🔗 Relazioni bidirezionali Record↔Date create: {relationships_created // 2} coppie")
        self.logger.info(f"   ↔️ Record→Date (hasModificationDate) + Date→Record (isModificationDateOf)")
        self.logger.info(f"   🏷️ Ogni entità Date ha rico:type 'Derived from embedded metadata'")
        self.logger.info(f"   📝 Ogni entità Date ha 4 proprietà: rdf:type, rico:type, normalizedDateValue, expressedDate")
        if existing_relationships_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {existing_relationships_skipped}")
        
        return triples
    
    
    def generate_record_filesystem_modification_date_triples(self, record_modification_date_records: List[RecordModificationDateRecord]) -> List[str]:
        """Genera triple RDF per date di modifica Record derivate da filesystem (fallback)"""
        self.logger.info("📅 GENERAZIONE TRIPLE DATE MODIFICA RECORD (FILESYSTEM FALLBACK)...")
        
        existing_records = self.get_existing_modification_date_relationships()
        
        triples = []
        date_entities_created = {}  # Traccia date create
        relationships_created = 0
        existing_relationships_skipped = 0
        
        for record in record_modification_date_records:
            # Salta se relazione già esiste
            if record.record_uri in existing_records:
                existing_relationships_skipped += 1
                continue
            
            # Crea entità Date (solo una volta per data unica)
            if record.date_uri not in date_entities_created:
                # Triple per l'entità Date secondo RiC-O
                triples.append(f"<{record.date_uri}> rdf:type rico:Date .")
                
                # rico:type per indicare la fonte FILESYSTEM
                triples.append(f'<{record.date_uri}> rico:type "Derived from file system metadata" .')
                
                # rico:normalizedDateValue per il valore standardizzato ISO 8601 (senza tipo XSD)
                triples.append(f"<{record.date_uri}> rico:normalizedDateValue \"{record.normalized_date}\" .")
                
                # rico:expressedDate in linguaggio naturale (formato: "01 January 2025")
                natural_date = self.format_date_natural_language(record.normalized_date)
                triples.append(f"<{record.date_uri}> rico:expressedDate \"{natural_date}\" .")
                
                date_entities_created[record.date_uri] = {
                    'normalized': record.normalized_date,
                    'natural': natural_date
                }
            
            # Triple per relazione bidirezionale Record ↔ Date
            triples.append(f"<{record.record_uri}> rico:hasModificationDate <{record.date_uri}> .")
            triples.append(f"<{record.date_uri}> rico:isModificationDateOf <{record.record_uri}> .")
            relationships_created += 2  # Relazione bidirezionale
        
        self.logger.info(f"✅ Generate {len(triples)} triple per date modifica Record filesystem fallback")
        self.logger.info(f"   📊 Entità Date uniche create: {len(date_entities_created)}")
        self.logger.info(f"   🔗 Relazioni bidirezionali Record↔Date create: {relationships_created // 2} coppie")
        self.logger.info(f"   ↔️ Record→Date (hasModificationDate) + Date→Record (isModificationDateOf)")
        self.logger.info(f"   🏷️ Ogni entità Date ha rico:type 'Derived from file system metadata'")
        self.logger.info(f"   📝 Ogni entità Date ha 4 proprietà: rdf:type, rico:type, normalizedDateValue, expressedDate")
        if existing_relationships_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {existing_relationships_skipped}")
        
        return triples

    def generate_recordset_modification_date_triples(self, recordset_modification_date_records: List[RecordSetModificationDateRecord]) -> List[str]:
        """Genera triple RDF per date di modifica RecordSet e relazioni bidirezionali seguendo RiC-O"""
        self.logger.info("📅 GENERAZIONE TRIPLE DATE MODIFICA RECORDSET...")
        
        existing_recordsets = self.get_existing_recordset_modification_date_relationships()
        
        triples = []
        date_entities_created = {}  # Traccia date create
        relationships_created = 0
        existing_relationships_skipped = 0
        
        for record in recordset_modification_date_records:
            # Salta se relazione già esiste
            if record.recordset_uri in existing_recordsets:
                existing_relationships_skipped += 1
                continue
            
            # Crea entità Date (solo una volta per data unica)
            if record.date_uri not in date_entities_created:
                # Triple per l'entità Date secondo RiC-O
                triples.append(f"<{record.date_uri}> rdf:type rico:Date .")
                
                # rico:normalizedDateValue per il valore standardizzato ISO 8601 (senza tipo XSD)
                triples.append(f"<{record.date_uri}> rico:normalizedDateValue \"{record.normalized_date}\" .")
                
                # rico:expressedDate in linguaggio naturale (formato: "01 January 2025")
                natural_date = self.format_date_natural_language(record.normalized_date)
                triples.append(f"<{record.date_uri}> rico:expressedDate \"{natural_date}\" .")
                
                date_entities_created[record.date_uri] = {
                    'normalized': record.normalized_date,
                    'natural': natural_date
                }
            
            # Triple per relazione bidirezionale RecordSet ↔ Date
            triples.append(f"<{record.recordset_uri}> rico:hasModificationDate <{record.date_uri}> .")
            triples.append(f"<{record.date_uri}> rico:isModificationDateOf <{record.recordset_uri}> .")
            relationships_created += 2  # Relazione bidirezionale
        
        self.logger.info(f"✅ Generate {len(triples)} triple per date modifica RecordSet (RiC-O compliant)")
        self.logger.info(f"   📊 Entità Date uniche create: {len(date_entities_created)}")
        self.logger.info(f"   🔗 Relazioni bidirezionali RecordSet↔Date create: {relationships_created // 2} coppie")
        self.logger.info(f"   ↔️ RecordSet→Date (hasModificationDate) + Date→RecordSet (isModificationDateOf)")
        self.logger.info(f"   📝 Ogni entità Date ha 3 proprietà: rdf:type, normalizedDateValue, expressedDate")
        if existing_relationships_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {existing_relationships_skipped}")
        
        return triples

    def generate_instantiation_modification_date_triples(self, instantiation_modification_date_records: List[InstantiationModificationDateRecord]) -> List[str]:
        """Genera triple RDF per date di modifica Instantiation e relazioni bidirezionali seguendo RiC-O"""
        self.logger.info("📅 GENERAZIONE TRIPLE DATE MODIFICA INSTANTIATION...")
        
        existing_instantiations = self.get_existing_instantiation_modification_date_relationships()
        
        triples = []
        date_entities_created = {}  # Traccia date create
        relationships_created = 0
        existing_relationships_skipped = 0
        
        for record in instantiation_modification_date_records:
            # Salta se relazione già esiste
            if record.instantiation_uri in existing_instantiations:
                existing_relationships_skipped += 1
                continue
            
            # Crea entità Date (solo una volta per data unica)
            if record.date_uri not in date_entities_created:
                # Triple per l'entità Date secondo RiC-O
                triples.append(f"<{record.date_uri}> rdf:type rico:Date .")
                
                # rico:normalizedDateValue per il valore standardizzato ISO 8601 (senza tipo XSD)
                triples.append(f"<{record.date_uri}> rico:normalizedDateValue \"{record.normalized_date}\" .")
                
                # rico:expressedDate in linguaggio naturale (formato: "01 January 2025")
                natural_date = self.format_date_natural_language(record.normalized_date)
                triples.append(f"<{record.date_uri}> rico:expressedDate \"{natural_date}\" .")
                
                date_entities_created[record.date_uri] = {
                    'normalized': record.normalized_date,
                    'natural': natural_date
                }
            
            # Triple per relazione bidirezionale Instantiation ↔ Date
            triples.append(f"<{record.instantiation_uri}> rico:hasModificationDate <{record.date_uri}> .")
            triples.append(f"<{record.date_uri}> rico:isModificationDateOf <{record.instantiation_uri}> .")
            relationships_created += 2  # Relazione bidirezionale
        
        self.logger.info(f"✅ Generate {len(triples)} triple per date modifica Instantiation (RiC-O compliant)")
        self.logger.info(f"   📊 Entità Date uniche create: {len(date_entities_created)}")
        self.logger.info(f"   🔗 Relazioni bidirezionali Instantiation↔Date create: {relationships_created // 2} coppie")
        self.logger.info(f"   ↔️ Instantiation→Date (hasModificationDate) + Date→Instantiation (isModificationDateOf)")
        self.logger.info(f"   📝 Ogni entità Date ha 3 proprietà: rdf:type, normalizedDateValue, expressedDate")
        if existing_relationships_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {existing_relationships_skipped}")
        
        return triples
    
    def find_instantiations_with_mime_types(self) -> List[MimeTypeRecord]:
        """Trova Instantiation con MIME types da classificare"""
        self.logger.info("🎭 RICERCA INSTANTIATION CON MIME TYPES...")
        
        # Query semplificata - solo il campo più comune per evitare timeout
        query = self.prefixes + """
        SELECT DISTINCT ?instantiation ?mimeType ?filePath WHERE {
                ?instantiation rdf:type rico:Instantiation .
                ?instantiation bodi:hasTechnicalMetadata ?metadata .
                ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                ?metadataType rdfs:label "Content-Type" .
                ?metadata rdf:value ?mimeType .
                
                OPTIONAL {
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?filePath .
                }
            
            # Filtri ottimizzati per velocità 
            FILTER(CONTAINS(STR(?mimeType), "/"))
            FILTER(STRLEN(STR(?mimeType)) > 3)
        }
        ORDER BY ?instantiation
        """
        
        try:
            start_time = time.time()
            self.logger.info("🔍 Esecuzione query per Content-Type...")
            
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=600  # Aumentato timeout a 10 minuti
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query MIME types fallita: HTTP {response.status_code}")
                if response.text:
                    self.logger.error(f"   Dettagli errore: {response.text[:300]}")
                return []
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            self.logger.info(f"✅ Query Content-Type completata in {query_time:.2f}s - Trovati {len(bindings)} risultati")
            
            # Se ci sono pochi risultati, prova anche gli altri campi
            if len(bindings) < 100:
                self.logger.info("🔍 Pochi risultati con Content-Type, provo altri campi...")
                additional_fields = ["Content-Type-Parser-Override", "Content-Type-Hint", "file_type"]
                
                for field in additional_fields:
                    additional_query = self.prefixes + f"""
                    SELECT DISTINCT ?instantiation ?mimeType ?filePath WHERE {{
                            ?instantiation rdf:type rico:Instantiation .
                            ?instantiation bodi:hasTechnicalMetadata ?metadata .
                            ?metadata bodi:hasTechnicalMetadataType ?metadataType .
                            ?metadataType rdfs:label "{field}" .
                            ?metadata rdf:value ?mimeType .
                            
                            OPTIONAL {{
                                ?instantiation prov:atLocation ?location .
                                ?location rdfs:label ?filePath .
                            }}
                        
                        FILTER(CONTAINS(STR(?mimeType), "/"))
                        FILTER(STRLEN(STR(?mimeType)) > 3)
                    }}
                    ORDER BY ?instantiation
                    """
                    
                    try:
                        response_add = self.session.post(
                            self.endpoint,
                            data={'query': additional_query},
                            timeout=300
                        )
                        
                        if response_add.status_code == 200:
                            additional_data = response_add.json()
                            additional_bindings = additional_data.get("results", {}).get("bindings", [])
                            bindings.extend(additional_bindings)
                            self.logger.info(f"   ✅ Campo {field}: +{len(additional_bindings)} risultati")
                        else:
                            self.logger.warning(f"   ⚠️ Campo {field}: query fallita")
                            
                    except Exception as e:
                        self.logger.warning(f"   ⚠️ Campo {field}: timeout o errore - {e}")
            
            # Rimuovi duplicati basati su instantiation_uri
            unique_bindings = {}
            for binding in bindings:
                instantiation_uri = binding["instantiation"]["value"]
                if instantiation_uri not in unique_bindings:
                    unique_bindings[instantiation_uri] = binding
            
            bindings = list(unique_bindings.values())
            self.logger.info(f"🔍 Dopo rimozione duplicati: {len(bindings)} istanziazioni uniche")
            
            mime_type_records = []
            
            for binding in bindings:
                instantiation_uri = binding["instantiation"]["value"]
                mime_type = binding["mimeType"]["value"].strip().lower()
                file_path = binding.get("filePath", {}).get("value", "Path not available")
                
                # Pulisci il MIME type (rimuovi parametri come charset)
                if ';' in mime_type:
                    mime_type = mime_type.split(';')[0].strip()
                
                # Validazione aggiuntiva in Python
                if '/' not in mime_type or len(mime_type) < 3:
                    continue
                    
                # Rimuovi valori non validi
                if ' ' in mime_type or '\n' in mime_type or '\t' in mime_type:
                    continue
                    
                # Skip se non è un MIME type riconoscibile
                if not any(mime_type.startswith(prefix) for prefix in ['image/', 'video/', 'audio/', 'text/', 'application/']):
                    continue
                
                # Classifica usando la mappatura esistente
                category = MIME_TYPE_CATEGORY_MAPPING.get(mime_type, "Unknown file")
                
                mime_type_record = MimeTypeRecord(
                    instantiation_uri=instantiation_uri,
                    mime_type=mime_type,
                    category=category,
                    file_path=file_path
                )
                
                mime_type_records.append(mime_type_record)
            
            self.logger.info(f"✅ Elaborazione completata")
            self.logger.info(f"📊 RISULTATI CLASSIFICAZIONE MIME TYPES:")
            self.logger.info(f"   📁 Instantiation con MIME types validi: {len(mime_type_records)}")
            
            if mime_type_records:
                # Statistiche per categoria
                category_stats = {}
                for record in mime_type_records:
                    category_stats[record.category] = category_stats.get(record.category, 0) + 1
                
                self.logger.info(f"   🎭 Categorie identificate: {len(category_stats)}")
                for category, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
                    self.logger.info(f"      • {category}: {count} file")
                
                # Mostra esempi
                self.logger.info("   📝 Esempi di classificazioni:")
                for i, record in enumerate(mime_type_records[:5]):
                    instantiation_short = record.instantiation_uri.split('/')[-1]
                    self.logger.info(f"      {i+1}. {instantiation_short}: {record.mime_type} → {record.category}")
                if len(mime_type_records) > 5:
                    self.logger.info(f"      ... e altri {len(mime_type_records) - 5} file")
            else:
                self.logger.info("   ℹ️ Nessun MIME type valido trovato nei metadati")
            
            return mime_type_records
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca MIME types: {e}")
            return []

    def get_existing_instantiation_types(self) -> Set[str]:
        """Ottiene tutte le Instantiation che hanno già rico:type"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO INSTANTIATION CON RICO:TYPE ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?instantiation WHERE {
            ?instantiation rdf:type rico:Instantiation .
            ?instantiation rico:type ?type .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query rico:type esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_instantiations = set()
            for binding in bindings:
                instantiation_uri = binding["instantiation"]["value"]
                existing_instantiations.add(instantiation_uri)
            
            self.logger.info(f"✅ Instantiation con rico:type già esistenti: {len(existing_instantiations)}")
            return existing_instantiations
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo rico:type esistenti: {e}")
            return set()

    def find_technical_metadata_types_for_sameas(self) -> Dict[str, str]:
        """Trova tutti i TechnicalMetadataType esistenti per creare relazioni owl:sameAs"""
        self.logger.info("🔗 RICERCA TECHNICAL METADATA TYPES PER OWL:SAMEAS...")
        
        query = self.prefixes + """
        SELECT DISTINCT ?metadataType ?label WHERE {
            GRAPH ?g {
                ?metadataType rdf:type bodi:TechnicalMetadataType .
                ?metadataType rdfs:label ?label .
            }
        }
        ORDER BY ?label
        """
        
        try:
            start_time = time.time()
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=300
            )
            query_time = time.time() - start_time
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query TechnicalMetadataType fallita: HTTP {response.status_code}")
                return {}
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            # Crea mappatura label -> URI
            label_to_uri = {}
            for binding in bindings:
                metadata_type_uri = binding["metadataType"]["value"]
                label = binding["label"]["value"]
                label_to_uri[label] = metadata_type_uri
            
            self.logger.info(f"✅ Query completata in {query_time:.2f}s")
            self.logger.info(f"📊 TechnicalMetadataType trovati: {len(label_to_uri):,}")
            
            # Verifica quante equivalenze possiamo creare
            potential_equivalences = 0
            for equivalence_group in METADATA_EQUIVALENCES:
                existing_in_group = [label for label in equivalence_group if label in label_to_uri]
                if len(existing_in_group) >= 2:
                    potential_equivalences += len(existing_in_group) * (len(existing_in_group) - 1)
            
            self.logger.info(f"🔗 Potenziali relazioni owl:sameAs da creare: {potential_equivalences:,}")
            
            return label_to_uri
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante ricerca TechnicalMetadataType: {e}")
            return {}

    def get_existing_sameas_relationships(self) -> Set[Tuple[str, str]]:
        """Ottiene tutte le relazioni owl:sameAs già esistenti tra TechnicalMetadataType"""
        if self.export_nquads:
            return set()
            
        self.logger.info("🔍 CONTROLLO RELAZIONI OWL:SAMEAS ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?type1 ?type2 WHERE {
            ?type1 owl:sameAs ?type2 .
            ?type1 rdf:type bodi:TechnicalMetadataType .
            ?type2 rdf:type bodi:TechnicalMetadataType .
        }
        """
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query owl:sameAs esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_relationships = set()
            for binding in bindings:
                type1 = binding["type1"]["value"]
                type2 = binding["type2"]["value"]
                existing_relationships.add((type1, type2))
            
            self.logger.info(f"✅ Relazioni owl:sameAs già esistenti: {len(existing_relationships):,}")
            return existing_relationships
            
        except Exception as e:
            self.logger.error(f"❌ Errore durante controllo owl:sameAs esistenti: {e}")
            return set()

    def generate_sameas_relationship_triples(self, label_to_uri: Dict[str, str]) -> List[str]:
        """Genera triple RDF per relazioni owl:sameAs tra TechnicalMetadataType equivalenti (solo se non esistenti)"""
        self.logger.info("🔗 GENERAZIONE RELAZIONI OWL:SAMEAS...")
        
        existing_relationships = self.get_existing_sameas_relationships()
        
        triples = []
        total_relationships = 0
        duplicate_relations_skipped = 0
        groups_processed = 0
        processed_pairs = set()  # Per evitare duplicati nella stessa esecuzione
        
        for equivalence_group in METADATA_EQUIVALENCES:
            # Trova i TechnicalMetadataType esistenti in questo gruppo di equivalenze
            existing_types = []
            for label in equivalence_group:
                if label in label_to_uri:
                    existing_types.append((label, label_to_uri[label]))
            
            # Se abbiamo almeno 2 tipi esistenti, crea relazioni owl:sameAs tra tutti
            if len(existing_types) >= 2:
                groups_processed += 1
                
                # Per ogni coppia nel gruppo (evitando duplicati)
                for i, (label1, uri1) in enumerate(existing_types):
                    for j, (label2, uri2) in enumerate(existing_types):
                        if i < j and uri1 != uri2:  # i < j evita duplicati e auto-relazioni
                            
                            # Crea una chiave ordinata per la coppia
                            pair_key = tuple(sorted([uri1, uri2]))
                            
                            # Salta se già processata in questa esecuzione
                            if pair_key in processed_pairs:
                                continue
                                
                            # Controlla se la relazione già esiste (in entrambe le direzioni)
                            if (uri1, uri2) not in existing_relationships and (uri2, uri1) not in existing_relationships:
                                # owl:sameAs è simmetrica, quindi creiamo solo una direzione per coppia
                                # Usiamo ordine lessicografico per consistenza
                                if uri1 < uri2:
                                    triples.append(f"<{uri1}> owl:sameAs <{uri2}> .")
                                else:
                                    triples.append(f"<{uri2}> owl:sameAs <{uri1}> .")
                                
                                total_relationships += 1
                                processed_pairs.add(pair_key)
                            else:
                                duplicate_relations_skipped += 1
                                processed_pairs.add(pair_key)
        
        self.logger.info(f"✅ Generate {total_relationships:,} nuove relazioni owl:sameAs")
        self.logger.info(f"   📊 Gruppi di equivalenza processati: {groups_processed}")
        self.logger.info(f"   🔗 Relazioni simmetriche create: {total_relationships}")
        if duplicate_relations_skipped > 0:
            self.logger.info(f"   ⚡ Relazioni già esistenti saltate: {duplicate_relations_skipped:,}")
        
        # Mostra esempi di relazioni create
        if triples:
            self.logger.info("   🔍 Esempi di relazioni owl:sameAs create:")
            example_count = min(5, len(triples))
            for i in range(example_count):
                triple = triples[i]
                # Estrai gli URI e mostra solo le label
                uri_parts = triple.split('> owl:sameAs <')
                if len(uri_parts) == 2:
                    uri1 = uri_parts[0].replace('<', '')
                    uri2 = uri_parts[1].replace('> .', '')
                    
                    # Trova le label corrispondenti
                    label1 = next((k for k, v in label_to_uri.items() if v == uri1), uri1.split('/')[-1])
                    label2 = next((k for k, v in label_to_uri.items() if v == uri2), uri2.split('/')[-1])
                    
                    self.logger.info(f"      {i+1}. '{label1}' ↔ '{label2}' (simmetrica)")
            
            if len(triples) > example_count:
                self.logger.info(f"      ... e altre {len(triples) - example_count:,} relazioni")
        
        return triples

    def generate_mime_type_classification_triples(self, mime_type_records: List[MimeTypeRecord]) -> List[str]:
        """Genera triple RDF per assegnare rico:type alle Instantiation basandosi sui MIME types"""
        self.logger.info("🎭 GENERAZIONE TRIPLE CLASSIFICAZIONE MIME TYPES...")
        
        existing_instantiations = self.get_existing_instantiation_types()
        
        triples = []
        classifications_created = 0
        existing_skipped = 0
        
        for record in mime_type_records:
            # Salta se già ha rico:type
            if record.instantiation_uri in existing_instantiations:
                existing_skipped += 1
                continue
            
            # Crea tripla rico:type con la categoria
            escaped_category = record.category.replace('"', '\\"')
            triple = f'<{record.instantiation_uri}> rico:type "{escaped_category}" .'
            triples.append(triple)
            classifications_created += 1
        
        self.logger.info(f"✅ Generate {len(triples)} triple per classificazione MIME types")
        self.logger.info(f"   📊 Nuove classificazioni create: {classifications_created}")
        if existing_skipped > 0:
            self.logger.info(f"   ⚡ Instantiation già classificate saltate: {existing_skipped}")
        
        # Statistiche per categoria
        if triples:
            category_counts = {}
            for record in mime_type_records:
                if record.instantiation_uri not in existing_instantiations:
                    category_counts[record.category] = category_counts.get(record.category, 0) + 1
            
            self.logger.info("   🎭 Nuove classificazioni per categoria:")
            for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"      • {category}: {count} instantiation")
        
        return triples

    def insert_triples(self, triples: List[str], operation_name: str, dry_run: bool = False) -> bool:
        """Inserisce triple nel grafo specificato (o simula se dry_run=True) e salva sempre in N-Quads"""
        if not triples:
            if dry_run:
                self.logger.info(f"🧪 DRY-RUN: Nessuna triple da inserire per {operation_name}")
            elif self.export_nquads:
                self.logger.info(f"💾 N-QUADS: Nessuna triple da esportare per {operation_name}")
            else:
                self.logger.warning(f"⚠️ Nessuna triple da inserire per {operation_name}")
            return True
        
        # SEMPRE aggiungi le triple al buffer N-Quads (se abilitato)
        if self.always_save_nquads or self.export_nquads:
            for triple in triples:
                nquad = self.convert_triple_to_nquads(triple)
                self.nquads_triples.append(nquad)
        
        if dry_run:
            self.logger.info(f"🧪 MODALITÀ DRY-RUN: Simulazione inserimento {operation_name}")
            self.logger.info(f"   📊 Triple che verrebbero create: {len(triples):,}")
            self.logger.info(f"   🎯 Nel grafo: <{self.target_graph}>")
            if self.always_save_nquads:
                self.logger.info(f"   💾 Triple aggiunte al buffer N-Quads: {len(triples):,}")
            self.logger.info("   🔍 Esempi di triple:")
            for i, triple in enumerate(triples[:5]):
                self.logger.info(f"      {i+1}. {triple}")
            if len(triples) > 5:
                self.logger.info(f"      ... e altre {len(triples) - 5:,} triple")
            return True
        
        if self.export_nquads:
            self.logger.info(f"💾 EXPORT N-QUADS: Aggiunta {len(triples):,} triple per {operation_name.upper()}")
            self.logger.info(f"🎯 Al grafo: <{self.target_graph}>")
            self.logger.info(f"   ✅ {len(triples):,} triple aggiunte al buffer N-Quads")
            return True
        
        # Inserimento in Blazegraph
        self.logger.info(f"💾 INSERIMENTO {len(triples):,} TRIPLE PER {operation_name.upper()}...")
        self.logger.info(f"🎯 Nel grafo: <{self.target_graph}>")
        if self.always_save_nquads:
            self.logger.info(f"💾 Salvate anche {len(triples):,} triple nel buffer N-Quads")
        
        batch_size = 1000
        batches = [triples[i:i + batch_size] for i in range(0, len(triples), batch_size)]
        
        successful_batches = 0
        total_inserted = 0
        
        start_time = time.time()
        
        for batch_num, batch in enumerate(batches, 1):
            try:
                triples_str = '\n    '.join(batch)
                
                # Query con GRAPH per inserire nel grafo specifico
                insert_query = f"""
                {self.prefixes}
                INSERT DATA {{
    GRAPH <{self.target_graph}> {{
        {triples_str}
    }}
                }}
                """
                
                response = self.session.post(
                    self.endpoint,
                    data={'update': insert_query},
                    timeout=120
                )
                
                if response.status_code == 200:
                    successful_batches += 1
                    total_inserted += len(batch)
                    self.logger.info(f"   ✅ Batch {batch_num}/{len(batches)}: {len(batch)} triple inserite")
                else:
                    self.logger.error(f"   ❌ Batch {batch_num}/{len(batches)} fallito: HTTP {response.status_code}")
                    if response.text:
                        self.logger.error(f"      Dettagli errore: {response.text[:200]}")
                    
            except Exception as e:
                self.logger.error(f"   ❌ Errore batch {batch_num}: {e}")
        
        insertion_time = time.time() - start_time
        success_rate = (successful_batches / len(batches)) * 100
        
        self.logger.info(f"📊 RIEPILOGO INSERIMENTO {operation_name.upper()}:")
        self.logger.info(f"   ✅ Batch riusciti: {successful_batches}/{len(batches)} ({success_rate:.1f}%)")
        self.logger.info(f"   📊 Triple inserite: {total_inserted:,}/{len(triples):,}")
        self.logger.info(f"   🎯 Nel grafo: <{self.target_graph}>")
        if self.always_save_nquads:
            self.logger.info(f"   💾 Triple salvate anche in N-Quads: {len(triples):,}")
        self.logger.info(f"   ⏱️ Tempo inserimento: {insertion_time:.2f}s")
        
        return successful_batches == len(batches)

    def run_enhanced_processing(self, process_hashes: bool = True, process_dates: bool = True, process_recordset_dates: bool = True, process_instantiation_dates: bool = True, process_record_modification_dates: bool = True, process_recordset_modification_dates: bool = True, process_instantiation_modification_dates: bool = True, process_mime_types: bool = True, process_sameas: bool = True, only_queries: bool = False, only_mappings: bool = False, download_csv: bool = True, csv_filename: str = None, dry_run: bool = False) -> ProcessingResult:
        """Esegue l'intero processo di elaborazione relazioni e date con nuovo ordine logico"""
        self.logger.info("🚀 AVVIO PROCESSO ELABORAZIONE AVANZATA")
        self.logger.info(f"📡 Endpoint: {self.endpoint}")
        self.logger.info(f"🎯 Grafo di destinazione: <{self.target_graph}>")
        
        if self.export_nquads:
            self.logger.info("💾 Modalità: SOLO EXPORT N-QUADS")
        else:
            self.logger.info(f"🧪 Modalità: {'DRY-RUN (simulazione)' if dry_run else 'PRODUZIONE (inserimento reale)'}")
            if self.always_save_nquads:
                self.logger.info("💾 File N-Quads: CREAZIONE AUTOMATICA ATTIVATA")

        # Determina operazioni basate sui gruppi
        if only_queries:
            # GRUPPO A: Solo operazioni basate su query  
            group_a_active = True
            group_b_active = False
            self.logger.info("🔍 GRUPPO A: Solo operazioni basate su query (hash, date, title)")
        elif only_mappings:
            # GRUPPO B: Solo operazioni con conoscenza esterna
            group_a_active = False
            group_b_active = True
            self.logger.info("🧠 GRUPPO B: Solo operazioni con conoscenza esterna (sameAs, sets, mime)")
        else:
            # Entrambi i gruppi
            group_a_active = True
            group_b_active = True
            self.logger.info("🔍🧠 ENTRAMBI I GRUPPI: Query + Conoscenza esterna")

        operations = []
        if group_a_active:
            group_a_ops = []
            if process_hashes:
                group_a_ops.append("Hash Duplicati")
            if process_dates:
                group_a_ops.append("Date Creazione Record")
            if process_recordset_dates:
                group_a_ops.append("Date Creazione RecordSet")
            if process_record_modification_dates:
                group_a_ops.append("Date Modifica Record")
            if process_recordset_modification_dates:
                group_a_ops.append("Date Modifica RecordSet")
            if process_instantiation_modification_dates:
                group_a_ops.append("Date Modifica Instantiation")
            group_a_ops.append("Title Generation")
            operations.extend(group_a_ops)
            
        if group_b_active:
            group_b_ops = []
            if process_sameas:
                group_b_ops.append("Relazioni owl:sameAs")
            group_b_ops.append("TechnicalMetadataTypeSet")
            group_b_ops.append("MetadataType-Set Relations")
            if process_mime_types:
                group_b_ops.append("Classificazione MIME Types")
            operations.extend(group_b_ops)
        
        self.logger.info(f"🔧 Operazioni: {', '.join(operations)}")
        self.logger.info("="*70)
        
        start_time = time.time()
        result = ProcessingResult()
        
        # Test connessione (saltato se export_nquads)
        if not self.test_connection():
            result.errors.append("Connessione a Blazegraph fallita")
            return result
        
        try:
            # =================================================================
            # GRUPPO A: ARRICCHIMENTO DA QUERY (solo dati esistenti nel grafo)  
            # =================================================================
            
            if not group_a_active:
                self.logger.info("⚡ SALTANDO GRUPPO A - Solo mappature richieste")
            else:
                self.logger.info("\n" + "🔍 GRUPPO A: ARRICCHIMENTO DA QUERY")
                self.logger.info("="*70)
                
                # === FASE 1A: HASH DUPLICATI ===
                if process_hashes:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("🔗 FASE 1A: HASH DUPLICATI")
                    self.logger.info("="*50)
                    
                    duplicate_groups = self.find_duplicate_hashes()
                    result.duplicate_groups = duplicate_groups
                    result.total_duplicate_groups = len(duplicate_groups)
                    result.total_instantiations_involved = sum(g.count for g in duplicate_groups)
                    
                    if duplicate_groups:
                        hash_triples = self.generate_hash_relationship_triples(duplicate_groups)
                        if hash_triples:
                            success = self.insert_triples(hash_triples, "relazioni hash", dry_run)
                            if success:
                                result.total_hash_relationships_created = len(hash_triples) if not dry_run else 0
                            else:
                                result.errors.append("Inserimento relazioni hash fallito")
                    else:
                        self.logger.info("✅ Nessun hash duplicato trovato")
                
                # === FASE 2A: DATE CREAZIONE RECORD ===
                if process_dates:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("📅 FASE 2A: DATE CREAZIONE RECORD")
                    self.logger.info("="*50)
                    
                    creation_date_records = self.find_records_with_creation_dates()
                    result.creation_date_records = creation_date_records
                    result.total_records_with_dates = len(creation_date_records)
                    
                    if creation_date_records:
                        date_triples = self.generate_creation_date_triples(creation_date_records)
                        if date_triples:
                            success = self.insert_triples(date_triples, "date creazione Record", dry_run)
                            if success:
                                date_entities = len([t for t in date_triples if "rdf:type rico:Date" in t])
                                date_relationships = len([t for t in date_triples if "rico:hasCreationDate" in t or "rico:isCreationDateOf" in t])
                                if not dry_run:
                                    result.total_date_entities_created = date_entities
                                    result.total_date_relationships_created = date_relationships
                            else:
                                result.errors.append("Inserimento date creazione Record fallito")
                    else:
                        self.logger.info("✅ Nessun Record con date di creazione trovato")
                
                
                
                # === FASE 5A: DATE MODIFICA RECORD ===
                if process_record_modification_dates:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("📅 FASE 5A: DATE MODIFICA RECORD")
                    self.logger.info("="*50)
                    
                    # Prima cerca dcterms:modified
                    record_modification_date_records = self.find_records_with_modification_dates()
                    result.record_modification_date_records = record_modification_date_records
                    result.total_records_with_modification_dates = len(record_modification_date_records)
                    
                    if record_modification_date_records:
                        record_modification_date_triples = self.generate_record_modification_date_triples(record_modification_date_records)
                        if record_modification_date_triples:
                            success = self.insert_triples(record_modification_date_triples, "date modifica Record (dcterms)", dry_run)
                            if success:
                                record_modification_date_relationships = len([t for t in record_modification_date_triples if "rico:hasModificationDate" in t or "rico:isModificationDateOf" in t])
                                if not dry_run:
                                    result.total_record_modification_date_relationships_created = record_modification_date_relationships
                            else:
                                result.errors.append("Inserimento date modifica Record (dcterms) fallito")
                    else:
                        self.logger.info("✅ Nessun Record con dcterms:modified trovato")
                    
                    # FALLBACK: cerca Record senza dcterms:modified ma con st_mtime
                    self.logger.info("\n" + "📁 FASE 5A-FALLBACK: DATE MODIFICA RECORD DA FILESYSTEM")
                    
                    record_filesystem_modification_date_records = self.find_records_with_filesystem_modification_dates()
                    
                    if record_filesystem_modification_date_records:
                        record_filesystem_modification_date_triples = self.generate_record_filesystem_modification_date_triples(record_filesystem_modification_date_records)
                        if record_filesystem_modification_date_triples:
                            success = self.insert_triples(record_filesystem_modification_date_triples, "date modifica Record (filesystem fallback)", dry_run)
                            if success:
                                filesystem_relationships = len([t for t in record_filesystem_modification_date_triples if "rico:hasModificationDate" in t or "rico:isModificationDateOf" in t])
                                if not dry_run:
                                    result.total_record_modification_date_relationships_created += filesystem_relationships
                            else:
                                result.errors.append("Inserimento date modifica Record (filesystem fallback) fallito")
                        
                        # Aggiungi ai risultati per il report
                        result.record_modification_date_records.extend(record_filesystem_modification_date_records)
                        result.total_records_with_modification_dates += len(record_filesystem_modification_date_records)
                    else:
                        self.logger.info("✅ Nessun Record aggiuntivo con st_mtime fallback trovato")
                
                # === FASE 6A: DATE MODIFICA RECORDSET ===
                if process_recordset_modification_dates:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("📅 FASE 6A: DATE MODIFICA RECORDSET")
                    self.logger.info("="*50)
                    
                    recordset_modification_date_records = self.find_recordsets_with_modification_dates()
                    result.recordset_modification_date_records = recordset_modification_date_records
                    result.total_recordsets_with_modification_dates = len(recordset_modification_date_records)
                    
                    if recordset_modification_date_records:
                        recordset_modification_date_triples = self.generate_recordset_modification_date_triples(recordset_modification_date_records)
                        if recordset_modification_date_triples:
                            success = self.insert_triples(recordset_modification_date_triples, "date modifica RecordSet", dry_run)
                            if success:
                                recordset_modification_date_relationships = len([t for t in recordset_modification_date_triples if "rico:hasModificationDate" in t or "rico:isModificationDateOf" in t])
                                if not dry_run:
                                    result.total_recordset_modification_date_relationships_created = recordset_modification_date_relationships
                            else:
                                result.errors.append("Inserimento date modifica RecordSet fallito")
                    else:
                        self.logger.info("✅ Nessun RecordSet con date di modifica trovato")
                
                # === FASE 7A: DATE MODIFICA INSTANTIATION ===
                if process_instantiation_modification_dates:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("📅 FASE 7A: DATE MODIFICA INSTANTIATION")
                    self.logger.info("="*50)
                    
                    instantiation_modification_date_records = self.find_instantiations_with_modification_dates()
                    result.instantiation_modification_date_records = instantiation_modification_date_records
                    result.total_instantiations_with_modification_dates = len(instantiation_modification_date_records)
                    
                    if instantiation_modification_date_records:
                        instantiation_modification_date_triples = self.generate_instantiation_modification_date_triples(instantiation_modification_date_records)
                        if instantiation_modification_date_triples:
                            success = self.insert_triples(instantiation_modification_date_triples, "date modifica Instantiation", dry_run)
                            if success:
                                instantiation_modification_date_relationships = len([t for t in instantiation_modification_date_triples if "rico:hasModificationDate" in t or "rico:isModificationDateOf" in t])
                                if not dry_run:
                                    result.total_instantiation_modification_date_relationships_created = instantiation_modification_date_relationships
                            else:
                                result.errors.append("Inserimento date modifica Instantiation fallito")
                    else:
                        self.logger.info("✅ Nessuna Instantiation con date di modifica trovata")
                
                # === FASE 8A: TITLE GENERATION ===
                self.logger.info("\n" + "="*50)
                self.logger.info("📝 FASE 8A: GENERAZIONE TITLE")
                self.logger.info("="*50)

                title_records = self.find_records_and_recordsets_for_titles()
                result.title_records = title_records
                result.total_titles_created = len(title_records)

                if title_records:
                    title_triples = self.generate_title_triples(title_records)
                    if title_triples:
                        success = self.insert_triples(title_triples, "Title generation", dry_run)
                        if success:
                            if not dry_run:
                                result.total_title_relationships_created = len([t for t in title_triples if "rico:hasOrHadTitle" in t or "rico:isTitleOf" in t])
                        else:
                            result.errors.append("Inserimento Title fallito")
                else:
                    self.logger.info("✅ Nessun Record/RecordSet trovato per Title")

            # =================================================================
            # GRUPPO B: ARRICCHIMENTO CON CONOSCENZA ESTERNA (dizionari/logica)
            # =================================================================
            
            if not group_b_active:
                self.logger.info("\n⚡ SALTANDO GRUPPO B - Solo query richieste")
            else:
                self.logger.info("\n" + "GRUPPO B: ARRICCHIMENTO CON CONOSCENZA ESTERNA")
                self.logger.info("="*70)
                
                # === FASE 1B: ALLINEAMENTO TECHNICAL METADATA TYPES ===
                if process_sameas:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("🔗 FASE 1B: ALLINEAMENTO TECHNICAL METADATA TYPES (OWL:SAMEAS)")
                    self.logger.info("="*50)
                    
                    label_to_uri = self.find_technical_metadata_types_for_sameas()
                    
                    if label_to_uri:
                        sameas_triples = self.generate_sameas_relationship_triples(label_to_uri)
                        if sameas_triples:
                            success = self.insert_triples(sameas_triples, "relazioni owl:sameAs", dry_run)
                            if success:
                                if not dry_run:
                                    if not hasattr(result, 'total_sameas_relationships_created'):
                                        result.total_sameas_relationships_created = 0
                                    result.total_sameas_relationships_created = len(sameas_triples)
                            else:
                                result.errors.append("Inserimento relazioni owl:sameAs fallito")
                        else:
                            self.logger.info("✅ Nessuna relazione owl:sameAs da creare")
                    else:
                        self.logger.info("✅ Nessun TechnicalMetadataType trovato per owl:sameAs")

                # === FASE 2B: SUDDIVISIONE IN METADATA SETS ===
                self.logger.info("\n" + "="*50)
                self.logger.info("🗂️ FASE 2B: CREAZIONE TECHNICAL METADATA SETS")
                self.logger.info("="*50)

                metadata_set_records = self.create_technical_metadata_sets()
                result.technical_metadata_set_records = metadata_set_records
                result.total_technical_metadata_sets_created = len(metadata_set_records)

                if metadata_set_records:
                    metadata_set_triples = self.generate_technical_metadata_set_triples(metadata_set_records)
                    if metadata_set_triples:
                        success = self.insert_triples(metadata_set_triples, "TechnicalMetadataTypeSet generation", dry_run)
                        if not success:
                            result.errors.append("Inserimento TechnicalMetadataTypeSet fallito")
                else:
                    self.logger.info("✅ Nessun TechnicalMetadataTypeSet da creare")

                # === FASE 3B: COLLEGAMENTO METADATATYPE AI METADATA SETS ===
                self.logger.info("\n" + "="*50)
                self.logger.info("🔗 FASE 3B: RELAZIONI METADATATYPE ↔ METADATASET")
                self.logger.info("="*50)

                metadata_type_set_triples = self.create_metadata_type_to_set_relationships()
                if metadata_type_set_triples:
                    success = self.insert_triples(metadata_type_set_triples, "MetadataType-Set relationships", dry_run)
                    if not success:
                        result.errors.append("Inserimento relazioni MetadataType-Set fallito")
                else:
                    self.logger.info("✅ Nessuna relazione MetadataType-Set da creare")

                # === FASE 4B: CLUSTERING MIMETYPE PER CLASSIFICAZIONE INSTANTIATION ===
                if process_mime_types:
                    self.logger.info("\n" + "="*50)
                    self.logger.info("🎭 FASE 4B: CLUSTERING MIMETYPE → RICO:TYPE SU INSTANTIATION")
                    self.logger.info("="*50)
                    
                    mime_type_records = self.find_instantiations_with_mime_types()
                    result.mime_type_records = mime_type_records
                    result.total_mime_type_classifications = len(mime_type_records)
                    
                    if mime_type_records:
                        mime_type_triples = self.generate_mime_type_classification_triples(mime_type_records)
                        if mime_type_triples:
                            success = self.insert_triples(mime_type_triples, "classificazione MIME types", dry_run)
                            if success:
                                if not dry_run:
                                    result.total_mime_type_relationships_created = len(mime_type_triples)
                            else:
                                result.errors.append("Inserimento classificazioni MIME types fallito")
                    else:
                        self.logger.info("✅ Nessuna Instantiation con MIME types da classificare trovata")
            
            # === SALVATAGGIO AUTOMATICO N-QUADS ===
            if (self.always_save_nquads or self.export_nquads) and self.nquads_triples:
                self.logger.info("\n" + "="*50)
                self.logger.info("💾 SALVATAGGIO AUTOMATICO FILE N-QUADS")
                self.logger.info("="*50)
                
                nquads_filename = self.save_nquads_to_file()
                if nquads_filename:
                    result.nquads_file = nquads_filename
                    result.total_nquads_written = len(self.nquads_triples)
                    self.logger.info(f"✅ File N-Quads creato automaticamente: {nquads_filename}")
                else:
                    result.errors.append("Salvataggio automatico N-Quads fallito")
            
            result.processing_time_seconds = time.time() - start_time
            
            # === RIEPILOGO FINALE ===
            self.logger.info("\n" + "="*70)
            self.logger.info("📋 RIEPILOGO FINALE ELABORAZIONE AVANZATA")
            self.logger.info("="*70)
            self.logger.info(f"⏱️ Tempo elaborazione totale: {result.processing_time_seconds:.2f} secondi")
            self.logger.info(f"🎯 Grafo di destinazione: <{self.target_graph}>")
            
            if self.always_save_nquads or self.export_nquads:
                self.logger.info(f"💾 File N-Quads:")
                self.logger.info(f"   📄 File generato: {result.nquads_file}")
                self.logger.info(f"   📊 Triple totali: {result.total_nquads_written:,}")
                if self.export_nquads:
                    self.logger.info(f"   📝 Modalità: SOLO export (non inserito in Blazegraph)")
                else:
                    self.logger.info(f"   📝 Modalità: Backup automatico + inserimento Blazegraph")
            
            # Riepilogo Gruppo A
            if group_a_active:
                self.logger.info(f"\n🔍 GRUPPO A - ARRICCHIMENTO DA QUERY:")
                
                if process_hashes:
                    self.logger.info(f"   🔗 Hash Duplicati: {result.total_duplicate_groups} gruppi, {result.total_instantiations_involved:,} file, {result.total_hash_relationships_created:,} relazioni")
                
                if process_dates:
                    self.logger.info(f"   📅 Date Creazione Record: {result.total_records_with_dates} Record, {result.total_date_relationships_created} relazioni")
                
                if process_recordset_dates:
                    self.logger.info(f"   📅 Date Creazione RecordSet: {result.total_recordsets_with_dates} RecordSet, {result.total_recordset_date_relationships_created} relazioni")
                
                
                if process_record_modification_dates:
                    self.logger.info(f"   📅 Date Modifica Record: {result.total_records_with_modification_dates} Record, {result.total_record_modification_date_relationships_created} relazioni")
                
                if process_recordset_modification_dates:
                    self.logger.info(f"   📅 Date Modifica RecordSet: {result.total_recordsets_with_modification_dates} RecordSet, {result.total_recordset_modification_date_relationships_created} relazioni")
                
                if process_instantiation_modification_dates:
                    self.logger.info(f"   📅 Date Modifica Instantiation: {result.total_instantiations_with_modification_dates} Instantiation, {result.total_instantiation_modification_date_relationships_created} relazioni")
                
                if len(result.title_records) > 0:
                    self.logger.info(f"   📝 Title: {len(result.title_records)} entità elaborate, {result.total_title_relationships_created} relazioni")
                
                if process_dates or process_recordset_dates or process_instantiation_dates or process_record_modification_dates or process_recordset_modification_dates or process_instantiation_modification_dates:
                    total_date_entities = result.total_date_entities_created
                    self.logger.info(f"   🗓️ Entità Date totali create: {total_date_entities}")

            # Riepilogo Gruppo B  
            if group_b_active:
                self.logger.info(f"\n GRUPPO B - ARRICCHIMENTO CON CONOSCENZA ESTERNA:")
                
                if hasattr(result, 'total_sameas_relationships_created') and result.total_sameas_relationships_created > 0:
                    self.logger.info(f"   🔗 Relazioni owl:sameAs: {result.total_sameas_relationships_created} collegamenti semantici")
                
                if len(result.technical_metadata_set_records) > 0:
                    self.logger.info(f"   🗂️ TechnicalMetadataTypeSet: {result.total_technical_metadata_sets_created} sets creati")
                    self.logger.info(f"   🔗 MetadataType-Set Relations: elaborate nella Fase 3B")
                
                if len(result.mime_type_records) > 0:
                    self.logger.info(f"   🎭 Classificazione MIME: {result.total_mime_type_classifications} Instantiation, {result.total_mime_type_relationships_created} proprietà rico:type")
                        
            if result.errors:
                self.logger.warning(f"\n⚠️ Errori riscontrati: {len(result.errors)}")
                for error in result.errors:
                    self.logger.warning(f"   • {error}")
            else:
                self.logger.info("\n🎉 PROCESSO COMPLETATO SENZA ERRORI!")
            
            self.logger.info("="*70)
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Errore fatale durante processo: {e}")
            result.errors.append(f"Errore fatale: {e}")
            result.processing_time_seconds = time.time() - start_time
            return result
                  

    def save_enhanced_report(self, result: ProcessingResult, filename: str = None):
        """Salva report dettagliato in formato JSON"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"enhanced_relationship_report_{timestamp}.json"
        
        report = {
            'generation_info': {
                'timestamp': datetime.now().isoformat(),
                'endpoint': self.endpoint,
                'target_graph': self.target_graph,
                'export_nquads': self.export_nquads,
                'nquads_file': result.nquads_file,
                'total_nquads_written': result.total_nquads_written,
                'processing_time_seconds': result.processing_time_seconds,
                'success': len(result.errors) == 0,
                'version': '5.2-nquads'
            },
            'title_generation': {
                'total_titles': result.total_titles_created,
                'relationships_created': result.total_title_relationships_created
            },
            'technical_metadata_sets': {
                'total_sets_created': result.total_technical_metadata_sets_created
            },
            'summary': {
                'hash_duplicates': {
                    'total_groups': result.total_duplicate_groups,
                    'total_instantiations': result.total_instantiations_involved,
                    'relationships_created': result.total_hash_relationships_created
                },
                'creation_dates_records': {
                    'total_records': result.total_records_with_dates,
                    'date_entities_created': result.total_date_entities_created,
                    'relationships_created': result.total_date_relationships_created
                },
                'modification_dates_records': {
                    'total_records': result.total_records_with_modification_dates,
                    'relationships_created': result.total_record_modification_date_relationships_created
                },
                'modification_dates_recordsets': {
                    'total_recordsets': result.total_recordsets_with_modification_dates,
                    'relationships_created': result.total_recordset_modification_date_relationships_created
                },
                'modification_dates_instantiations': {
                    'total_instantiations': result.total_instantiations_with_modification_dates,
                    'relationships_created': result.total_instantiation_modification_date_relationships_created
                },
                'errors_count': len(result.errors)
            },
            'mime_type_classification': {
                'total_instantiations': result.total_mime_type_classifications,
                'relationships_created': result.total_mime_type_relationships_created
            },
            'errors': result.errors,
            'duplicate_groups': [
                {
                    'hash_value': group.hash_value,
                    'duplicate_count': group.count,
                    'relationships_created': group.relationships_to_create,
                    'instantiations': group.instantiations,
                    'file_paths': group.paths
                }
                for group in result.duplicate_groups
            ],
            'creation_date_records': [
                {
                    'record_uri': record.record_uri,
                    'instantiation_uri': record.instantiation_uri,
                    'original_date_value': record.metadata_value,
                    'normalized_date': record.normalized_date,
                    'date_uri': record.date_uri,
                    'file_path': record.file_path
                }
                for record in result.creation_date_records
            ],
            'title_records': [
                {
                    'entity_uri': record.entity_uri,
                    'entity_type': record.entity_type,
                    'label_value': record.label_value,
                    'title_uri': record.title_uri
                }
                for record in result.title_records
            ],
            'technical_metadata_set_records': [
                {
                    'set_uri': record.set_uri,
                    'set_label': record.set_label,
                    'set_type': record.set_type
                }
                for record in result.technical_metadata_set_records
            ],
            'record_modification_date_records': [
                {
                    'record_uri': record.record_uri,
                    'instantiation_uri': record.instantiation_uri,
                    'original_date_value': record.metadata_value,
                    'normalized_date': record.normalized_date,
                    'date_uri': record.date_uri,
                    'file_path': record.file_path
                }
                for record in result.record_modification_date_records
            ],
            'recordset_modification_date_records': [
                {
                    'recordset_uri': record.recordset_uri,
                    'instantiation_uri': record.instantiation_uri,
                    'original_date_value': record.metadata_value,
                    'normalized_date': record.normalized_date,
                    'date_uri': record.date_uri,
                    'file_path': record.file_path
                }
                for record in result.recordset_modification_date_records
            ],
            'instantiation_modification_date_records': [
                {
                    'instantiation_uri': record.instantiation_uri,
                    'original_date_value': record.metadata_value,
                    'normalized_date': record.normalized_date,
                    'date_uri': record.date_uri,
                    'file_path': record.file_path
                }
                for record in result.instantiation_modification_date_records
            ],
            'mime_type_records': [
                {
                    'instantiation_uri': record.instantiation_uri,
                    'mime_type': record.mime_type,
                    'category': record.category,
                    'file_path': record.file_path
                }
                for record in result.mime_type_records
            ]
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"📄 Report salvato: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio report: {e}")
            return None

def parse_arguments():
    """Parser degli argomenti da riga di comando"""
    parser = argparse.ArgumentParser(
        description="Enhanced Relationship and Date Generator - Generatore Avanzato Relazioni e Date con supporto grafo e N-Quads",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:

  # Esecuzione normale - inserisce in Blazegraph E crea file N-Quads
  python enhanced_relationship_generator.py
  
  # Simulazione completa (raccomandato per il primo test) + file N-Quads 
  python enhanced_relationship_generator.py --dry-run
  
  # SOLO export N-Quads (non inserisce in Blazegraph) 
  python enhanced_relationship_generator.py --export-nquads
  
  
  # Inserimento normale SENZA file N-Quads automatico
  python enhanced_relationship_generator.py --no-auto-nquads
  
  # Solo hash duplicati + file N-Quads automatico
  python enhanced_relationship_generator.py --only-hashes


COMPORTAMENTO AUTOMATICO N-QUADS:
- Per DEFAULT, ogni esecuzione crea AUTOMATICAMENTE un file .nq con tutte le triple generate
- Il file viene salvato con timestamp: relations_update_YYYYMMDD_HHMMSS.nq
- Questo succede sia in modalità dry-run che in produzione
- Puoi disabilitare questo comportamento con --no-auto-nquads
"""
    )
    
    parser.add_argument(
        '--endpoint',
        default='http://localhost:10214/blazegraph/namespace/kb/sparql',
        help='Endpoint SPARQL di Blazegraph (default: localhost:10214)'
    )   
    
    parser.add_argument(
        '--target-graph',
        default='http://ficlit.unibo.it/ArchivioEvangelisti/updated_relations',
        help='URI del grafo di destinazione per le nuove triple (default: http://ficlit.unibo.it/ArchivioEvangelisti/updated_relations)'
    )
    
    parser.add_argument(
        '--export-nquads',
        action='store_true',
        help='Esporta le triple SOLO in formato N-Quads invece di inserirle in Blazegraph'
    )
    
    parser.add_argument(
        '--no-auto-nquads',
        action='store_true',
        help='Disabilita il salvataggio automatico del file N-Quads (default: sempre attivo)'
    )
    
    
    parser.add_argument(
        '--nquads-file',
        help='Nome file N-Quads personalizzato (default: auto-generato con timestamp)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modalità simulazione - analizza ma non crea relazioni'
    )
    
    parser.add_argument(
        '--test-only',
        action='store_true',
        help='Esegue solo il test di connessione'
    )
    
    parser.add_argument(
        '--only-hashes',
        action='store_true',
        help='Elabora solo hash duplicati (non date creazione)'
    )
    
    parser.add_argument(
        '--only-dates',
        action='store_true',
        help='Elabora solo date di creazione Record (dcterms:created)'
    )
    
    
    parser.add_argument(
        '--only-record-modification-dates',
        action='store_true',
        help='Elabora solo date di modifica Record (dcterms:modified)'
    )
    
    parser.add_argument(
        '--only-recordset-modification-dates',
        action='store_true',
        help='Elabora solo date di modifica RecordSet (st_mtime)'
    )
    
    parser.add_argument(
        '--only-instantiation-modification-dates',
        action='store_true',
        help='Elabora solo date di modifica Instantiation (st_mtime)'
    )
    
    parser.add_argument(
        '--only-mime-types',
        action='store_true',
        help='Elabora solo classificazione MIME types (non altro)'
    )

    parser.add_argument(
        '--only-sameas',
        action='store_true',
        help='Elabora solo relazioni owl:sameAs tra TechnicalMetadataType equivalenti'
    )

    parser.add_argument(
    '--only-queries',
    action='store_true',
    help='Esegue solo operazioni basate su query del Gruppo A (hash, date, title)'
    )

    parser.add_argument(
        '--only-mappings', 
        action='store_true',
        help='Esegue solo operazioni con conoscenza esterna del Gruppo B (sameAs, sets, mime)'
    )
        
    parser.add_argument(
        '--report-file',
        help='Nome file per salvare il report (default: auto-generato)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Output verboso con dettagli aggiuntivi'
    )
    
    return parser.parse_args()


def main():
    """Funzione principale"""
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='[%(levelname)s] %(asctime)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    print("= - relations_update_graph.py:3526"*70)
    print("🔗 ENHANCED RELATIONSHIP AND DATE GENERATOR  ARCHIVIO EVANGELISTI - relations_update_graph.py:3527")
    print("🔧 Generatore Avanzato per Relazioni Hash e Date di Creazione/Modifica - relations_update_graph.py:3528")
    print("🎯 CON SUPPORTO GRAFO SPECIFICO E NQUADS AUTOMATICO - relations_update_graph.py:3529")
    print("= - relations_update_graph.py:3530"*70)
    print(f"📡 Endpoint Blazegraph: {args.endpoint} - relations_update_graph.py:3531")
    print(f"🎯 Grafo di destinazione: {args.target_graph} - relations_update_graph.py:3532")
    
    always_save_nquads = not args.no_auto_nquads
    
    if args.export_nquads:
        print("💾 Modalità: SOLO EXPORT NQUADS (non inserisce in Blazegraph) - relations_update_graph.py:3537")
        if args.nquads_file:
            print(f"📄 File NQuads: {args.nquads_file} - relations_update_graph.py:3539")
        else:
            print("📄 File NQuads: autogenerato con timestamp - relations_update_graph.py:3541")
    else:
        if args.dry_run:
            print("🧪 Modalità: DRYRUN (solo simulazione) - relations_update_graph.py:3544")
        else:
            print("🔴 Modalità: PRODUZIONE (inserimento in Blazegraph) - relations_update_graph.py:3546")
        
        if always_save_nquads:
            print("💾 File NQuads: CREATO AUTOMATICAMENTE per backup - relations_update_graph.py:3549")
            if args.nquads_file:
                print(f"📄 Nome file: {args.nquads_file} - relations_update_graph.py:3551")
            else:
                print("📄 Nome file: autogenerato con timestamp - relations_update_graph.py:3553")
        else:
            print("💾 File NQuads: DISABILITATO (noautonquads) - relations_update_graph.py:3555")

    
    # ===================================================================
    # INIZIALIZZAZIONE VARIABILI DI CONTROLLO OPERAZIONI
    # ===================================================================
    
    # Inizializza tutte le variabili a True per default (esegue tutto)
    process_hashes = True
    process_dates = True
    process_recordset_dates = True
    process_instantiation_dates = True
    process_record_modification_dates = True
    process_recordset_modification_dates = True
    process_instantiation_modification_dates = True
    process_mime_types = True
    process_sameas = True
    
    # Determina operazioni da eseguire basandosi sui parametri
        # Determina operazioni da eseguire basandosi sui parametri
    specific_operations = any([
        args.only_hashes, args.only_dates, 
        args.only_record_modification_dates, 
        args.only_recordset_modification_dates, args.only_instantiation_modification_dates,
        args.only_mime_types, args.only_sameas
    ])

    group_operations = args.only_queries or args.only_mappings

    # Se sono specificati parametri --only-*, disabilita tutto e abilita solo quello specifico
    if specific_operations:
        process_hashes = args.only_hashes
        process_dates = args.only_dates
        process_recordset_dates = False  # Non esiste più come argomento
        process_instantiation_dates = False  # Non esiste più come argomento
        process_record_modification_dates = args.only_record_modification_dates
        process_recordset_modification_dates = args.only_recordset_modification_dates
        process_instantiation_modification_dates = args.only_instantiation_modification_dates
        process_mime_types = args.only_mime_types
        process_sameas = args.only_sameas
    
    # Se specificato --only-queries (GRUPPO A), disabilita Gruppo B
    elif args.only_queries:
        # GRUPPO A: Solo operazioni basate su query  
        process_hashes = True
        process_dates = True
        process_recordset_dates = True
        process_instantiation_dates = True
        process_record_modification_dates = True
        process_recordset_modification_dates = True
        process_instantiation_modification_dates = True
        # Title è incluso automaticamente nel Gruppo A
        process_sameas = False
        process_mime_types = False
    
    # Se specificato --only-mappings (GRUPPO B), disabilita Gruppo A
    elif args.only_mappings:
        # GRUPPO B: Solo operazioni con conoscenza esterna
        process_hashes = False
        process_dates = False
        process_recordset_dates = False
        process_instantiation_dates = False
        process_record_modification_dates = False
        process_recordset_modification_dates = False
        process_instantiation_modification_dates = False
        # TechnicalMetadataTypeSet e MetadataType-Set Relations sono inclusi automaticamente nel Gruppo B
        process_sameas = True
        process_mime_types = True
    
    # Altrimenti esegue tutto (default)
    # Le variabili sono già tutte True dall'inizializzazione

    # ===================================================================
    # PREPARAZIONE OUTPUT OPERAZIONI
    # ===================================================================
    
    # Organizza operazioni per gruppo per output
    group_a_operations = []
    group_b_operations = []

    if process_hashes:
        group_a_operations.append("Hash Duplicati")
    if process_dates:
        group_a_operations.append("Date Creazione Record")
    if process_recordset_dates:
        group_a_operations.append("Date Creazione RecordSet")
    if process_record_modification_dates:
        group_a_operations.append("Date Modifica Record")
    if process_recordset_modification_dates:
        group_a_operations.append("Date Modifica RecordSet")
    if process_instantiation_modification_dates:
        group_a_operations.append("Date Modifica Instantiation")
    
    # Title è sempre nel Gruppo A (se non specificato --only-mappings)
    if not args.only_mappings:
        group_a_operations.append("Title Generation")

    if process_sameas:
        group_b_operations.append("Relazioni owl:sameAs")
    
    # TechnicalMetadataTypeSet e relazioni sono sempre nel Gruppo B (se non specificato --only-queries)
    if not args.only_queries:
        group_b_operations.append("TechnicalMetadataTypeSet")
        group_b_operations.append("MetadataType-Set Relations")
    
    if process_mime_types:
        group_b_operations.append("Classificazione MIME Types")

    # Mostra operazioni per gruppo
    if args.only_queries:
        print(f"🔧 GRUPPO A (Query): {', '.join(group_a_operations)} - relations_update_graph.py:3664")
    elif args.only_mappings:
        print(f"🔧 GRUPPO B (Mappature): {', '.join(group_b_operations)} - relations_update_graph.py:3666")
    elif specific_operations:
        all_enabled_operations = []
        if process_hashes: all_enabled_operations.append("Hash Duplicati")
        if process_dates: all_enabled_operations.append("Date Creazione Record")
        if process_recordset_dates: all_enabled_operations.append("Date Creazione RecordSet")
        if process_record_modification_dates: all_enabled_operations.append("Date Modifica Record")
        if process_recordset_modification_dates: all_enabled_operations.append("Date Modifica RecordSet")
        if process_instantiation_modification_dates: all_enabled_operations.append("Date Modifica Instantiation")
        if process_sameas: all_enabled_operations.append("Relazioni owl:sameAs")
        if process_mime_types: all_enabled_operations.append("Classificazione MIME Types")
        all_enabled_operations.append("Title Generation")
        all_enabled_operations.append("TechnicalMetadataTypeSet")
        all_enabled_operations.append("MetadataType-Set Relations")
        print(f"🔧 OPERAZIONI SPECIFICHE: {', '.join(all_enabled_operations)} - relations_update_graph.py:3680")
    else:
        print(f"🔧 GRUPPO A (Query): {', '.join(group_a_operations)} - relations_update_graph.py:3682")
        print(f"🔧 GRUPPO B (Mappature): {', '.join(group_b_operations)} - relations_update_graph.py:3683")
    
    if args.dry_run:
        print("⚠️ MODALITÀ SICURA: Nessuna modifica ai dati, solo analisi - relations_update_graph.py:3686")
        if always_save_nquads:
            print("💾 File NQuads creato automaticamente per review - relations_update_graph.py:3688")
    elif args.export_nquads:
        print("💾 MODALITÀ NQUADS: Le triple verranno salvate SOLO in file - relations_update_graph.py:3690")
    else:
        print("🔴 MODALITÀ PRODUZIONE: Le relazioni verranno inserite nel grafo specificato - relations_update_graph.py:3692")
        if always_save_nquads:
            print("💾 + File NQuads creato automaticamente per backup - relations_update_graph.py:3694")
    print("= - relations_update_graph.py:3695"*70)
    
    try:
        # Crea generatore con grafo personalizzato e supporto N-Quads
        generator = EnhancedRelationshipGenerator(
            args.endpoint, 
            args.target_graph,
            args.export_nquads,
            always_save_nquads
        )
        
        # Test connessione
        if not generator.test_connection():
            print("❌ Impossibile connettersi a Blazegraph - relations_update_graph.py:3708")
            sys.exit(1)
        
        if args.test_only:
            print("✅ TEST CONNESSIONE COMPLETATO CON SUCCESSO - relations_update_graph.py:3712")
            if not args.export_nquads:
                print("📡 Server Blazegraph raggiungibile e operativo - relations_update_graph.py:3714")
            print(f"🎯 Grafo di destinazione configurato: <{args.target_graph}> - relations_update_graph.py:3715")
            print("🚀 Pronto per eseguire l'elaborazione avanzata - relations_update_graph.py:3716")
            return
        
        # Esegue il processo con le operazioni determinate
        result = generator.run_enhanced_processing(
            process_hashes=process_hashes, 
            process_dates=process_dates, 
            process_recordset_dates=process_recordset_dates, 
            process_instantiation_dates=process_instantiation_dates, 
            process_record_modification_dates=process_record_modification_dates, 
            process_recordset_modification_dates=process_recordset_modification_dates, 
            process_instantiation_modification_dates=process_instantiation_modification_dates,
            process_mime_types=process_mime_types,
            process_sameas=process_sameas,
            only_queries=args.only_queries,
            only_mappings=args.only_mappings,
            dry_run=args.dry_run
        )
        
        # Salva N-Quads se richiesto manualmente (oltre a quello automatico)
        if args.export_nquads and args.nquads_file and not result.nquads_file:
            nquads_filename = generator.save_nquads_to_file(args.nquads_file)
            if nquads_filename:
                result.nquads_file = nquads_filename
                result.total_nquads_written = len(generator.nquads_triples)
        
        # Salva report
        has_results = (result.total_duplicate_groups > 0 or 
            result.total_records_with_dates > 0 or 
            result.total_recordsets_with_dates > 0 or 
            result.total_instantiations_with_dates > 0 or 
            result.total_records_with_modification_dates > 0 or 
            result.total_recordsets_with_modification_dates > 0 or 
            result.total_instantiations_with_modification_dates > 0 or
            result.total_mime_type_classifications > 0 or
            (hasattr(result, 'total_sameas_relationships_created') and result.total_sameas_relationships_created > 0))
        
        if args.report_file or has_results:
            generator.save_enhanced_report(result, args.report_file)
        
        # Exit code basato sui risultati
        if result.errors:
            print(f"⚠️ Uscita con codice 1: {len(result.errors)} errori riscontrati - relations_update_graph.py:3758")
            sys.exit(1)
        elif not has_results: 
            print("✅ Uscita con codice 0: Nessun elemento da elaborare trovato - relations_update_graph.py:3761")
            if result.nquads_file:
                print(f"💾 File NQuads vuoto comunque creato: {result.nquads_file} - relations_update_graph.py:3763")
            sys.exit(0)
        else:
            print("✅ Uscita con codice 0: Processo completato con successo - relations_update_graph.py:3766")
            if result.nquads_file:
                print(f"💾 File NQuads generato automaticamente: {result.nquads_file} - relations_update_graph.py:3768")
                print(f"📊 Triple totali nel file: {result.total_nquads_written:,} - relations_update_graph.py:3769")
            if not args.export_nquads and not args.dry_run:
                print(f"🎯 Triple inserite anche nel grafo: <{args.target_graph}> - relations_update_graph.py:3771")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n⚠️ Processo interrotto dall'utente - relations_update_graph.py:3775")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Errore fatale: {e} - relations_update_graph.py:3778")
        sys.exit(1)


if __name__ == "__main__":
    main()


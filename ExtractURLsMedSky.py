import os
import csv
from atproto import Client, models

def save_extracted_fields_to_csv(threads, output_file):
    """Extract and save fields to a CSV file, appending if the file exists.
    
    Args:
        threads: List of threads, where each thread contains posts with content and metadata
        output_file: Path to CSV file where data will be saved
    """
    file_exists = os.path.isfile(output_file)
    with open(output_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write the header only if the file is newly created
        if not file_exists:
            writer.writerow(["Extracted Text", "Post Link"])

        for thread in threads:
            for post in thread:
                # Extract text before the arrow symbol (->) as it contains the relevant content
                content = post['content']
                if "->" in content:
                    extracted = content.split('->')[0].strip()
                    post_link = f"https://bsky.app/profile/{post['author_handle']}/post/{post['uri'].split('/')[-1]}"
                    writer.writerow([extracted, post_link])


def fetch_parent_post_links(profile_url, did, profile_handle, client):
    """Fetch links to parent posts containing the word 'thread'.
    
    Args:
        profile_url: URL of the Bluesky profile to fetch posts from
        did: Decentralized identifier of the profile
        profile_handle: Handle of the profile (e.g. 'medsky.social')
        client: Authenticated Bluesky API client
        
    Returns:
        List of URLs to parent posts that contain 'thread' in their content
    """
    parent_post_links = []
    cursor = None
    while True:
        feed_params = models.AppBskyFeedGetAuthorFeed.Params(actor=did, cursor=cursor)
        response = client.app.bsky.feed.get_author_feed(feed_params)
        feed = response.feed

        # Filter posts containing the word 'thread' to find parent posts of labeling threads
        for item in feed:
            content = getattr(item.post.record, "text", "")
            if "thread" in content.lower():
                uri_parts = item.post.uri.split("/")
                post_id = uri_parts[-1]
                parent_post_links.append(f"https://bsky.app/profile/{profile_handle}/post/{post_id}")

        # Handle pagination - break if no more pages
        cursor = response.cursor
        if not cursor:
            break

    return parent_post_links


def process_post(uri, client, depth=0):
    """Recursively process a post and all its replies to extract content.
    
    Args:
        uri: URI of the post to process
        client: Authenticated Bluesky API client
        depth: Current depth in the reply tree (for debugging)
        
    Returns:
        List of dictionaries containing post content and metadata
    """
    # Fetch a post by its URI
    post_params = models.AppBskyFeedGetPostThread.Params(uri=uri)
    response = client.app.bsky.feed.get_post_thread(post_params)
    
    thread = response.thread
    if not thread or not hasattr(thread, "post"):
        return []

    # Extract the main post content and metadata
    post_content = {
        "author_handle": thread.post.author.handle,
        "author_did": thread.post.author.did,
        "content": getattr(thread.post.record, "text", ""),
        "uri": thread.post.uri,
    }
    result = [post_content]

    # Recursively process all replies in the thread
    if hasattr(thread, "replies") and thread.replies:
        for reply in thread.replies:
            reply_uri = reply.post.uri
            print(f"|--Processing uri {reply_uri}")
            result.extend(process_post(reply_uri, client, depth + 1))

    return result
    

def fetch_post_content_and_replies_iteratively(post_url, did, client):
    """Process a post and all its replies to extract content.
    
    Args:
        post_url: URL of the post to process
        did: Decentralized identifier of the post author
        client: Authenticated Bluesky API client
        
    Returns:
        List of all posts in the thread with their content and metadata
    """
    # Extract profile handle and post ID from the post URL
    parts = post_url.split('/')
    post_id = parts[-1]

    # Convert web URL to Bluesky URI format and process the thread
    main_post_uri = f"at://{did}/app.bsky.feed.post/{post_id}"
    all_posts = process_post(main_post_uri, client)

    return all_posts


def main():
    profile_url = 'https://bsky.app/profile/medsky.social/'  # Profile URL
    output_file = os.path.join(os.getcwd(), 'extracted_fields_medsky.csv')

    # Extract profile handle and resolve DID once
    profile_handle = 'medsky.social'
    client = Client()

    # Retrieve authentication credentials from environment variables
    user = os.getenv("BSKY_USER")
    password = os.getenv("BSKY_PASSWORD")
    if not password:
        raise EnvironmentError("Environment variable BSKY_PASSWORD not set. Please export it in your shell.")

    client.login(user, password)

    # Resolve the DID (decentralized identifier) for the profile
    params = models.ComAtprotoIdentityResolveHandle.Params(handle=profile_handle)
    profile = client.com.atproto.identity.resolve_handle(params)
    did = profile.did
    print(f"Resolved DID for {profile_handle}: {did}")

    # Fetch parent posts that start labeling threads
    parent_post_links = fetch_parent_post_links(profile_url, did, profile_handle, client)
    print(f"Found parent posts: {parent_post_links}")

    # Process each parent post and its replies to extract labeled content
    threads = []
    for i, post_url in enumerate(parent_post_links):
        print(f"Processing parent post {i} / {len(parent_post_links)}: {post_url}")
        all_posts = fetch_post_content_and_replies_iteratively(post_url, did, client)
        threads.append(all_posts)
    save_extracted_fields_to_csv(threads, output_file)

    print(f"Data saved to {output_file}")

if __name__ == '__main__':
    main()

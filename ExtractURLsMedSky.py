import os
import csv
from atproto import Client, models

def save_extracted_fields_to_csv(threads, output_file):
    """Extract and save fields to a CSV file, appending if the file exists."""
    file_exists = os.path.isfile(output_file)
    with open(output_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write the header only if the file is newly created
        if not file_exists:
            writer.writerow(["Extracted Text", "Post Link"])

        for thread in threads:
            for post in thread:
                # Extract the desired substring
                content = post['content']
                if "->" in content:
                    extracted = content.split('->')[0].strip()
                    post_link = f"https://bsky.app/profile/{post['author_handle']}/post/{post['uri'].split('/')[-1]}"
                    writer.writerow([extracted, post_link])


def fetch_parent_post_links(profile_url, did, profile_handle, client):
    """Fetch links to parent posts containing the word 'thread'."""
    # Fetch posts from the author's feed
    parent_post_links = []
    cursor = None
    while True:
        feed_params = models.AppBskyFeedGetAuthorFeed.Params(actor=did, cursor=cursor)
        response = client.app.bsky.feed.get_author_feed(feed_params)
        feed = response.feed

        # Filter posts containing the word 'thread'.
        # Note that this is not a fool proof way / specific for the medsky labeler
        for item in feed:
            content = getattr(item.post.record, "text", "")
            if "thread" in content.lower():
                uri_parts = item.post.uri.split("/")
                post_id = uri_parts[-1]
                parent_post_links.append(f"https://bsky.app/profile/{profile_handle}/post/{post_id}")

        # Handle pagination
        cursor = response.cursor
        if not cursor:
            break

    return parent_post_links


# Recursive function to fetch replies iteratively
def process_post(uri, client, depth=0):
    # Fetch a post by its URI
    post_params = models.AppBskyFeedGetPostThread.Params(uri=uri)
    response = client.app.bsky.feed.get_post_thread(post_params)
    
    thread = response.thread
    if not thread or not hasattr(thread, "post"):
        return []

    # Extract the main post content
    post_content = {
        "author_handle": thread.post.author.handle,
        "author_did": thread.post.author.did,
        "content": getattr(thread.post.record, "text", ""),
        "uri": thread.post.uri,
    }
    result = [post_content]

    # Process replies one by one
    if hasattr(thread, "replies") and thread.replies:
        for reply in thread.replies:
            reply_uri = reply.post.uri
            print(f"|--Processing uri {reply_uri}")
            result.extend(process_post(reply_uri, client, depth + 1))  # Recursive call for each reply

    return result
    

def fetch_post_content_and_replies_iteratively(post_url, did, client):
    """Process a post and its replies, saving extracted data to a CSV."""

    # Extract profile handle and post ID from the post URL
    parts = post_url.split('/')
    post_id = parts[-1]

    # Start processing from the main post
    main_post_uri = f"at://{did}/app.bsky.feed.post/{post_id}"
    all_posts = process_post(main_post_uri, client)

    return all_posts


def main():
    profile_url = 'https://bsky.app/profile/medsky.social/'  # Profile URL
    output_file = os.path.join(os.getcwd(), 'extracted_fields_medsky.csv')

    # Extract profile handle and resolve DID once
    profile_handle = 'medsky.social'
    client = Client()

   # Retrieve password from the environment variable
    user = os.getenv("BSKY_USER")
    password = os.getenv("BSKY_PASSWORD")
    if not password:
        raise EnvironmentError("Environment variable BSKY_PASSWORD not set. Please export it in your shell.")

    client.login(user, password)

    params = models.ComAtprotoIdentityResolveHandle.Params(handle=profile_handle)
    profile = client.com.atproto.identity.resolve_handle(params)
    did = profile.did
    print(f"Resolved DID for {profile_handle}: {did}")

    # Fetch parent post links containing the word 'thread'
    parent_post_links = fetch_parent_post_links(profile_url, did, profile_handle, client)
    print(f"Found parent posts: {parent_post_links}")

    # Process each parent post link
    threads = []
    for i, post_url in enumerate(parent_post_links):
        print(f"Processing parent post {i} / {len(parent_post_links)}: {post_url}")
        all_posts = fetch_post_content_and_replies_iteratively(post_url, did, client)
        # Save extracted fields to CSV
        threads.append(all_posts)
    save_extracted_fields_to_csv(threads, output_file)


    print(f"Data saved to {output_file}")

if __name__ == '__main__':
    main()
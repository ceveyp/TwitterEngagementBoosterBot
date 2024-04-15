import datetime
from datetime import timedelta
import multiprocessing
import re
import asyncio
from discord.utils import get
from discord.ext import tasks
from engagements_notifier import *

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


@client.event
async def on_ready():
    clean_up_setup_channels.start()
    print(f'We have logged in as {client.user}')


async def leaderboard(message: discord.Message):
    channel = message.channel
    pages = []
    leader_board_pages = get_leaderboard_pages()
    page_count = len(leader_board_pages)
    i = 1
    for leader_board_page in leader_board_pages:
        embed = discord.Embed(title="GS Points Leaderboard",
                              colour=0x003399)
        for field in leader_board_page:
            embed.add_field(name=field.get('user'), value=field.get('points'), inline=False)
        embed.set_footer(text=f"Page {i}/{page_count}")
        embed.set_thumbnail(url="https://cdn.discordapp.com/avatars/1223669327648264273/0b777d8b0ee3fe2580edcc4d55e4dfcc.jpg")
        i += 1
        pages.append(embed)
    index = 0
    message = await channel.send(embed=pages[0])
    emojis = ["◀️", "▶️"]
    for emoji in emojis:
        await message.add_reaction(emoji)
    while not client.is_closed():
        try:
            react, user = await client.wait_for("reaction_add",
                                                timeout=60.0,
                                                check=lambda r, u: r.emoji in emojis and r.message.id == message.id)
            if react.emoji == emojis[0] and index > 0:
                index -= 1
            elif react.emoji == emojis[1] and index < len(pages) - 1:
                index += 1
            await message.edit(embed=pages[index])
        except asyncio.TimeoutError:
            break


def get_leaderboard_pages() -> list:
    sql_query = """SELECT username, points 
                        FROM {}.users 
                        WHERE points>0
                        ORDER BY points DESC
                        LIMIT 30""".format(mysql_db_name)
    leaderboard_users = mysql_query(sql_query)
    page_count = -(-len(leaderboard_users) // 10)
    if page_count == 1:
        leaderboard_users = [leaderboard_users]
    if page_count == 2:
        leaderboard_users = [leaderboard_users[0:10],
                             leaderboard_users[10:len(leaderboard_users)]]
    if page_count == 3:
        leaderboard_users = [leaderboard_users[0:10],
                             leaderboard_users[10:20],
                             leaderboard_users[30:len(leaderboard_users)]]
    pages = []
    i = 1
    for page in range(0, page_count):
        page_fields = []
        for leaderboard_user in leaderboard_users[page]:
            if i == 1:
                medal = ':first_place_medal:'
            elif i == 2:
                medal = ':second_place_medal:'
            elif i == 3:
                medal = ':third_place_medal:'
            else:
                medal = ':medal:'
            username = (str(leaderboard_user.get("username")))[0:15].ljust(15, '⠀')
            points = "Points: " + str(leaderboard_user.get("points"))
            embed_fields = {"user": "{}⠀-⠀{}{}".format(str(i), medal, username),
                            "points": points}
            page_fields.append(embed_fields)
            i += 1
        pages.append(page_fields)
    return pages


async def make_channel(ctx):
    setup_embed_text = "Hello {}! \n\n **Please enter twitter username with this command: /register username:**"
    channel_created_text = "{} please register your Twitter username in the <#{}> channel! :trophy:"
    guild = ctx.guild
    member = ctx.author
    channel = ctx.channel
    category = discord.utils.get(ctx.guild.categories, name=twitter_setup_group_name)
    default_role_perms = discord.PermissionOverwrite(read_messages=False)
    member_perms = discord.PermissionOverwrite(read_messages=True)
    overwrites = {guild.default_role: default_role_perms,
                  member: member_perms}
    setup_channel = await guild.create_text_channel('setup', overwrites=overwrites, category=category)
    await channel.send(channel_created_text.format(member.mention, setup_channel.id))
    setup_embed_text = setup_embed_text.format(member.mention)
    setup_embed = discord.Embed(title="Register To Earn Points For Engaging!",
                                description=setup_embed_text,
                                color=embed_color)
    await setup_channel.send(embed=setup_embed)


async def register_user(message):
    member = message.author
    channel = message.channel

    if registered_user_role_name in [x.name.lower() for x in member.roles]:
        await channel.send(registration_exists_message.format(member.mention))
        return

    try:
        twitter_handle = message.content.split()[1].strip()
        if not re.match(r'^[a-zA-Z0-9_]{1,15}$', twitter_handle):
            await channel.send(register_default_message)
            return
    except Exception as e:
        print(e)
        await channel.send(register_default_message)
        return
    sql_query = """REPLACE INTO {}.users (user_id, username, twitter_handle)
                        VALUES (%s, %s, %s)""".format(mysql_db_name)
    params = (member.id, member.name, twitter_handle)
    if not mysql_exec(sql_query, params):
        await channel.send(register_error_message)
        return

    # Assign role
    registered_user_role = get(member.guild.roles, name=registered_user_role_name)
    await member.add_roles(registered_user_role)

    # Send embed
    register_embed = discord.Embed(description=register_success_message.format(member.mention),
                                   color=embed_color)
    await channel.send(embed=register_embed)


@tasks.loop(minutes=15)
async def clean_up_setup_channels():
    all_channels = client.get_all_channels()
    for channel in all_channels:
        if channel.name == "setup":
            now_timestamp = int(datetime.utcnow().timestamp())
            expired_timestamp = int((channel.created_at + timedelta(minutes=5)).timestamp())
            if now_timestamp > expired_timestamp:
                await channel.delete()


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    member = message.author
    channel = message.channel

    if message.channel.name.endswith('setup-points'):
        if message.content.strip() == '/earnpoints':
            if registered_user_role_name in [x.name for x in member.roles]:
                await channel.send(registration_exists_message.format(member.mention))
                return
            await make_channel(message)
        if message.content.strip() == '/points-leaderboard':
            await leaderboard(message)

    if message.channel.name == 'setup':
        if not message.content.startswith('/register'):
            await message.channel.send(register_default_message)
        else:
            await register_user(message)


if __name__ == '__main__':
    multiprocessing.Process(target=run_twitter_engagements_notifier).start()
    multiprocessing.Process(target=run_twitter_new_posts_notifier).start()
    client.run(token)
